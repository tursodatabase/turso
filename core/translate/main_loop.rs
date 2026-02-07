use turso_parser::ast::{Expr, SortOrder};

use super::{
    aggregation::{translate_aggregation_step, AggArgumentSource},
    emitter::{
        MaterializedBuildInputMode, MaterializedColumnRef, OperationMode, TranslateCtx,
        UpdateRowSource,
    },
    expr::{
        translate_condition_expr, translate_expr, translate_expr_no_constant_opt, walk_expr,
        ConditionMetadata, NoConstantOptReason, WalkControl,
    },
    group_by::{group_by_agg_phase, GroupByMetadata, GroupByRowSource},
    optimizer::{constraints::BinaryExprSide, Optimizable},
    order_by::{order_by_sorter_insert, sorter_insert},
    plan::{
        Aggregate, DistinctCtx, Distinctness, EvalAt, HashJoinOp, HashJoinType, IterationDirection,
        JoinOrderMember, JoinedTable, MultiIndexScanOp, NonFromClauseSubquery, Operation,
        QueryDestination, Scan, Search, SeekDef, SeekKey, SeekKeyComponent, SelectPlan,
        SetOperation, TableReferences, WhereTerm,
    },
};
use crate::{
    emit_explain,
    schema::{Index, Table},
    translate::{
        collate::{get_collseq_from_expr, resolve_comparison_collseq, CollationSeq},
        emitter::{prepare_cdc_if_necessary, HashCtx},
        expr::comparison_affinity,
        planner::{table_mask_from_expr, TableMask},
        result_row::emit_select_result,
        subquery::emit_non_from_clause_subquery,
        window::emit_window_loop_source,
    },
    turso_assert, turso_assert_eq,
    types::SeekOp,
    vdbe::{
        affinity::{self, Affinity},
        builder::{
            CursorKey, CursorType, HashBuildSignature, MaterializedBuildInputModeTag,
            ProgramBuilder, QueryMode,
        },
        insn::{to_u16, CmpInsFlags, HashBuildData, IdxInsertFlags, Insn},
        BranchOffset, CursorID,
    },
    Result,
};
use std::{borrow::Cow, collections::HashSet, sync::Arc};
use turso_macros::turso_assert_some;

// Metadata for handling LEFT JOIN operations
#[derive(Debug)]
pub struct LeftJoinMetadata {
    // integer register that holds a flag that is set to true if the current row has a match for the left join
    pub reg_match_flag: usize,
    // label for the instruction that sets the match flag to true
    pub label_match_flag_set_true: BranchOffset,
    // label for the instruction that checks if the match flag is true
    pub label_match_flag_check_value: BranchOffset,
}

/// Jump labels for each loop in the query's main execution loop
#[derive(Debug, Clone, Copy)]
pub struct LoopLabels {
    /// jump to the start of the loop body
    pub loop_start: BranchOffset,
    /// jump to the Next instruction (or equivalent)
    pub next: BranchOffset,
    /// jump to the end of the loop, exiting it
    pub loop_end: BranchOffset,
}

impl LoopLabels {
    pub fn new(program: &mut ProgramBuilder) -> Self {
        Self {
            loop_start: program.allocate_label(),
            next: program.allocate_label(),
            loop_end: program.allocate_label(),
        }
    }
}

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
    // Include hash-join build tables so their cursors are opened for hash build.
    let mut required_tables: HashSet<usize> = join_order
        .iter()
        .map(|member| member.original_idx)
        .collect();
    for table in tables.joined_tables().iter() {
        if let Operation::HashJoin(hash_join_op) = &table.op {
            required_tables.insert(hash_join_op.build_table_idx);
        }
    }

    for (table_index, table) in tables.joined_tables().iter().enumerate() {
        if !required_tables.contains(&table_index) {
            continue;
        }
        // Initialize bookkeeping for OUTER JOIN
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_metadata = LeftJoinMetadata {
                    reg_match_flag: program.alloc_register(),
                    label_match_flag_set_true: program.allocate_label(),
                    label_match_flag_check_value: program.allocate_label(),
                };
                t_ctx.meta_left_joins[table_index] = Some(lj_metadata);
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
                                db: table.database_id,
                            });
                        }
                    }
                    if let Some(index_cursor_id) = index_cursor_id {
                        program.emit_insn(Insn::OpenWrite {
                            cursor_id: index_cursor_id,
                            root_page: index.as_ref().unwrap().root_page.into(),
                            db: table.database_id,
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
                    if is_write && tbl.readonly() {
                        return Err(crate::LimboError::ReadOnly);
                    }
                    if let Some(cursor_id) = table_cursor_id {
                        program.emit_insn(Insn::VOpen { cursor_id });
                        if is_write {
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
                _ => panic!("only SELECT is supported for index method"),
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
                    _ => unreachable!("Multi-index scans should only occur in SELECT operations"),
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
            jump_target_when_false: t_ctx.label_main_loop_end.unwrap(),
            jump_target_when_null: t_ctx.label_main_loop_end.unwrap(),
        };
        translate_condition_expr(program, tables, &cond.expr, meta, &t_ctx.resolver)?;
        program.preassign_label_to_next_insn(jump_target);
    }

    Ok(())
}

/// Information captured during hash table build for use in probe emission.
///
/// This describes the payload layout, key affinities, bloom filter settings,
/// and whether the probe phase may seek into the build table for missing data.
#[derive(Debug, Clone)]
pub struct HashBuildPayloadInfo {
    /// Column references stored as payload, in order.
    /// These may reference multiple tables when using a materialized join prefix.
    pub payload_columns: Vec<MaterializedColumnRef>,
    /// Affinity string for the join keys.
    pub key_affinities: String,
    /// Whether to use a bloom filter for probe-side pruning.
    pub use_bloom_filter: bool,
    /// Cursor id used to store the bloom filter.
    pub bloom_filter_cursor_id: CursorID,
    /// Whether it's safe to SeekRowid into the build table when payload is missing.
    /// This is false when keys/payload are sourced from a materialized input.
    pub allow_seek: bool,
}

/// Emit the hash table build phase for a hash join operation.
///
/// This scans the build input (either the base table or a materialized input)
/// and populates the hash table with matching rows and payload. The returned
/// metadata drives probe-side emission, including bloom filter use and whether
/// build-table seeks are permitted.
/// Uses a separate hash build cursor so the build scan does not interfere with
/// the probe cursor for the same table.
#[allow(clippy::too_many_arguments)]
fn emit_hash_build_phase(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &TableReferences,
    non_from_clause_subqueries: &[NonFromClauseSubquery],
    predicates: &[WhereTerm],
    hash_join_op: &HashJoinOp,
    hash_build_cursor_id: CursorID,
    hash_table_id: usize,
) -> Result<HashBuildPayloadInfo> {
    let build_table = &table_references.joined_tables()[hash_join_op.build_table_idx];
    let btree = build_table
        .btree()
        .expect("Hash join build table must be a BTree table");
    // If build input was materialized, we may use either rowids (SeekRowid) or
    // precomputed keys/payload depending on the materialization mode.
    let materialized_input = t_ctx
        .materialized_build_inputs
        .get(&hash_join_op.build_table_idx);
    let materialized_cursor_id = materialized_input.map(|input| input.cursor_id);

    let num_keys = hash_join_op.join_keys.len();
    // Determine comparison affinity for each join key
    let mut key_affinities = String::new();
    for join_key in hash_join_op.join_keys.iter() {
        let build_expr = join_key.get_build_expr(predicates);
        let probe_expr = join_key.get_probe_expr(predicates);
        let affinity = comparison_affinity(build_expr, probe_expr, Some(table_references), None);
        key_affinities.push(affinity.aff_mask());
    }

    // Extract collations for each join key by examining both sides of the comparison.
    // This follows SQLite's collation precedence rules using the original LHS/RHS
    // of the comparison, not the build/probe assignment (which can be swapped by the
    // optimizer based on ORDER BY or cost estimates).
    let collations: Vec<CollationSeq> = hash_join_op
        .join_keys
        .iter()
        .map(|join_key| {
            let (original_lhs, original_rhs) = match join_key.build_side {
                BinaryExprSide::Lhs => (
                    join_key.get_build_expr(predicates),
                    join_key.get_probe_expr(predicates),
                ),
                BinaryExprSide::Rhs => (
                    join_key.get_probe_expr(predicates),
                    join_key.get_build_expr(predicates),
                ),
            };
            resolve_comparison_collseq(original_lhs, original_rhs, table_references)
                .unwrap_or(CollationSeq::Binary)
        })
        .collect();

    // Bloom filter uses binary hashing; skip if any join key uses non-binary collation.
    let use_bloom_filter = hash_join_op.use_bloom_filter
        && collations
            .iter()
            .all(|c| matches!(c, CollationSeq::Binary | CollationSeq::Unset));

    let (payload_columns, payload_signature_columns, use_materialized_keys, allow_seek) =
        match materialized_input.map(|input| &input.mode) {
            Some(MaterializedBuildInputMode::KeyPayload {
                num_keys: payload_num_keys,
                payload_columns,
            }) => {
                turso_assert!(
                    *payload_num_keys == num_keys,
                    "materialized hash build input key count mismatch"
                );
                let payload_signature_columns = (0..payload_columns.len())
                    .map(|i| *payload_num_keys + i)
                    .collect();
                (
                    payload_columns.clone(),
                    payload_signature_columns,
                    true,
                    false,
                )
            }
            _ => {
                // Collect payload columns from the build table for normal hash builds.
                let payload_signature_columns: Vec<usize> =
                    build_table.col_used_mask.iter().collect();
                let payload_columns: Vec<MaterializedColumnRef> = payload_signature_columns
                    .iter()
                    .map(|col_idx| {
                        let column = build_table
                            .columns()
                            .get(*col_idx)
                            .expect("build table column missing");
                        MaterializedColumnRef::Column {
                            table_id: build_table.internal_id,
                            column_idx: *col_idx,
                            is_rowid_alias: column.is_rowid_alias(),
                        }
                    })
                    .collect();
                (payload_columns, payload_signature_columns, false, true)
            }
        };
    let bloom_filter_cursor_id = if use_materialized_keys {
        materialized_cursor_id.expect("materialized input cursor is required")
    } else {
        hash_build_cursor_id
    };

    let join_key_indices = hash_join_op
        .join_keys
        .iter()
        .map(|key| key.where_clause_idx)
        .collect::<Vec<_>>();
    let signature = HashBuildSignature {
        join_key_indices,
        payload_refs: payload_columns.clone(),
        key_affinities: key_affinities.clone(),
        use_bloom_filter,
        materialized_input_cursor: materialized_cursor_id,
        materialized_mode: materialized_input.as_ref().map(|input| match input.mode {
            MaterializedBuildInputMode::RowidOnly => MaterializedBuildInputModeTag::RowidOnly,
            MaterializedBuildInputMode::KeyPayload { .. } => MaterializedBuildInputModeTag::Payload,
        }),
    };
    if program.hash_build_signature_matches(hash_table_id, &signature) {
        return Ok(HashBuildPayloadInfo {
            payload_columns,
            key_affinities,
            use_bloom_filter,
            bloom_filter_cursor_id,
            allow_seek,
        });
    }
    if program.has_hash_build_signature(hash_table_id) {
        program.emit_insn(Insn::HashClose { hash_table_id });
        program.clear_hash_build_signature(hash_table_id);
    }

    let build_key_start_reg = program.alloc_registers(num_keys);
    let mut build_rowid_reg = None;
    let mut build_iter_cursor_id = hash_build_cursor_id;
    if let Some(input) = materialized_input {
        match &input.mode {
            MaterializedBuildInputMode::RowidOnly => {
                build_rowid_reg = Some(program.alloc_register());
                build_iter_cursor_id = input.cursor_id;
            }
            MaterializedBuildInputMode::KeyPayload { .. } => {
                build_iter_cursor_id = input.cursor_id;
            }
        }
    }

    let (key_source_cursor_id, payload_source_cursor_id, hash_build_rowid_cursor_id) =
        if use_materialized_keys {
            (
                build_iter_cursor_id,
                build_iter_cursor_id,
                build_iter_cursor_id,
            )
        } else {
            (
                hash_build_cursor_id,
                hash_build_cursor_id,
                hash_build_cursor_id,
            )
        };

    // Create new loop for hash table build phase
    let build_loop_start = program.allocate_label();
    let build_loop_end = program.allocate_label();
    let skip_to_next = program.allocate_label();
    let label_hash_build_end = program.allocate_label();
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: label_hash_build_end,
    });

    // This is a separate cursor from the regular table cursor so we can iterate
    // the build side without disturbing the probe cursor.
    if !use_materialized_keys {
        program.emit_insn(Insn::OpenRead {
            cursor_id: hash_build_cursor_id,
            root_page: btree.root_page,
            db: build_table.database_id,
        });
    }

    program.emit_insn(Insn::Rewind {
        cursor_id: build_iter_cursor_id,
        pc_if_empty: build_loop_end,
    });

    // Set cursor override so translate_expr uses the hash build cursor for this table
    if !use_materialized_keys {
        program.set_cursor_override(build_table.internal_id, hash_build_cursor_id);
    }

    program.preassign_label_to_next_insn(build_loop_start);

    if let (Some(rowid_reg), Some(input_cursor_id)) = (build_rowid_reg, materialized_cursor_id) {
        program.emit_column_or_rowid(input_cursor_id, 0, rowid_reg);
        program.emit_insn(Insn::SeekRowid {
            cursor_id: hash_build_cursor_id,
            src_reg: rowid_reg,
            target_pc: skip_to_next,
        });
    }

    if !use_materialized_keys {
        let build_only_mask =
            TableMask::from_table_number_iter([hash_join_op.build_table_idx].into_iter());
        for cond in predicates.iter() {
            let mask =
                table_mask_from_expr(&cond.expr, table_references, non_from_clause_subqueries)?;
            if !mask.contains_table(hash_join_op.build_table_idx)
                || !build_only_mask.contains_all(&mask)
            {
                continue;
            }
            let jump_target_when_true = program.allocate_label();
            let condition_metadata = ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true,
                jump_target_when_false: skip_to_next,
                jump_target_when_null: skip_to_next,
            };
            translate_condition_expr(
                program,
                table_references,
                &cond.expr,
                condition_metadata,
                &t_ctx.resolver,
            )?;
            program.preassign_label_to_next_insn(jump_target_when_true);
        }
    }

    if use_materialized_keys {
        for idx in 0..num_keys {
            program.emit_column_or_rowid(key_source_cursor_id, idx, build_key_start_reg + idx);
        }
    } else {
        for (idx, join_key) in hash_join_op.join_keys.iter().enumerate() {
            let build_expr = join_key.get_build_expr(predicates);
            let target_reg = build_key_start_reg + idx;
            translate_expr(
                program,
                Some(table_references),
                build_expr,
                target_reg,
                &t_ctx.resolver,
            )?;
        }
    }

    // Apply affinity conversion to build keys before hashing
    if let Some(count) = std::num::NonZeroUsize::new(num_keys) {
        program.emit_insn(Insn::Affinity {
            start_reg: build_key_start_reg,
            count,
            affinities: key_affinities.clone(),
        });
    }

    let num_payload = payload_columns.len();

    let (payload_start_reg, mut payload_info) = if num_payload > 0 {
        let payload_reg = program.alloc_registers(num_payload);
        for (i, &col_idx) in payload_signature_columns.iter().enumerate() {
            program.emit_column_or_rowid(payload_source_cursor_id, col_idx, payload_reg + i);
        }
        (
            Some(payload_reg),
            HashBuildPayloadInfo {
                payload_columns,
                key_affinities: key_affinities.clone(),
                use_bloom_filter: false,
                bloom_filter_cursor_id,
                allow_seek,
            },
        )
    } else {
        (
            None,
            HashBuildPayloadInfo {
                payload_columns: vec![],
                key_affinities: key_affinities.clone(),
                use_bloom_filter: false,
                bloom_filter_cursor_id,
                allow_seek,
            },
        )
    };

    if !use_materialized_keys {
        program.clear_cursor_override(build_table.internal_id);
    }

    // Insert current row into hash table with payload columns.
    program.emit_insn(Insn::HashBuild {
        data: Box::new(HashBuildData {
            cursor_id: hash_build_rowid_cursor_id,
            key_start_reg: build_key_start_reg,
            num_keys,
            hash_table_id,
            mem_budget: hash_join_op.mem_budget,
            collations,
            payload_start_reg,
            num_payload,
            track_matched: matches!(
                hash_join_op.join_type,
                HashJoinType::LeftOuter | HashJoinType::FullOuter
            ),
        }),
    });
    if use_bloom_filter {
        program.emit_insn(Insn::FilterAdd {
            cursor_id: bloom_filter_cursor_id,
            key_reg: build_key_start_reg,
            num_keys,
        });
        payload_info.use_bloom_filter = true;
    }

    program.preassign_label_to_next_insn(skip_to_next);
    program.emit_insn(Insn::Next {
        cursor_id: build_iter_cursor_id,
        pc_if_next: build_loop_start,
    });

    program.preassign_label_to_next_insn(build_loop_end);
    program.emit_insn(Insn::HashBuildFinalize { hash_table_id });
    program.record_hash_build_signature(hash_table_id, signature);

    program.preassign_label_to_next_insn(label_hash_build_end);
    Ok(payload_info)
}

/// Emit bytecode for a multi-index scan (OR-by-union or AND-by-intersection).
///
/// For *Union (OR)*: scans each index branch separately, collecting rowids into a RowSet
/// for deduplication, then reads from the RowSet to fetch actual rows.
///
/// For *Intersection (AND)*: uses two RowSets. First branch populates rowset1,
/// subsequent branches test against rowset1 and add found rows to rowset2.
/// Only rowids appearing in ALL branches end up in the final result.
#[allow(clippy::too_many_arguments)]
fn emit_multi_index_scan_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table: &JoinedTable,
    table_references: &TableReferences,
    multi_idx_op: &MultiIndexScanOp,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
    _next: BranchOffset,
) -> Result<()> {
    let table_cursor_id = program.resolve_cursor_id(&CursorKey::table(table.internal_id));

    // Allocate register for rowid during collection and final fetch
    let rowid_reg = program.alloc_register();

    // For intersection, we need two rowsets (swap between them for multi-way intersection)
    // For union, we only need one rowset
    let is_intersection = multi_idx_op.set_op == SetOperation::Intersection;

    // Allocate registers for RowSets
    let rowset1_reg = program.alloc_register();
    let rowset2_reg = if is_intersection {
        program.alloc_register()
    } else {
        rowset1_reg // Union only uses one rowset
    };

    // Initialize RowSet(s) to NULL (empty)
    program.emit_insn(Insn::Null {
        dest: rowset1_reg,
        dest_end: None,
    });
    if is_intersection {
        program.emit_insn(Insn::Null {
            dest: rowset2_reg,
            dest_end: None,
        });
    }

    // For intersection, we track which rowset to read from and write to
    // First branch writes to rowset1, subsequent branches read from current and write to next
    let mut current_read_rowset = rowset1_reg;
    let mut current_write_rowset = if is_intersection {
        rowset2_reg
    } else {
        rowset1_reg
    };

    // Emit each index branch - collect rowids into RowSet
    for (branch_idx, branch) in multi_idx_op.branches.iter().enumerate() {
        let branch_loop_start = program.allocate_label();
        let branch_loop_end = program.allocate_label();
        let branch_next = program.allocate_label();

        // For intersection after first branch, we need a label for "found in previous rowset"
        let found_in_prev_label = if is_intersection && branch_idx > 0 {
            Some(program.allocate_label())
        } else {
            None
        };

        let seek_def = &branch.seek_def;
        let is_index = branch.index.is_some();
        let branch_cursor_id = if let Some(index) = &branch.index {
            program.resolve_cursor_id(&CursorKey::index(table.internal_id, index.clone()))
        } else {
            // Rowid-based access
            table_cursor_id
        };

        // Reuse regular seek emission so multi-index branches honor start/end bounds
        // and NULL-key behavior exactly the same as non-multi-index scans.
        let max_key_regs = seek_def
            .size(&seek_def.start)
            .max(seek_def.size(&seek_def.end))
            .max(1);
        let key_start_reg = program.alloc_registers(max_key_regs);
        emit_seek(
            program,
            table_references,
            seek_def,
            t_ctx,
            branch_cursor_id,
            key_start_reg,
            branch_loop_end,
            branch.index.as_ref(),
            false, // multi-index branches never use ephemeral autoindex bloom filters
        )?;
        emit_seek_termination(
            program,
            table_references,
            seek_def,
            t_ctx,
            branch_cursor_id,
            key_start_reg,
            branch_loop_start,
            branch_loop_end,
            branch.index.as_ref(),
        )?;

        // Get rowid from the branch cursor.
        if is_index {
            program.emit_insn(Insn::IdxRowId {
                cursor_id: branch_cursor_id,
                dest: rowid_reg,
            });
        } else {
            program.emit_insn(Insn::RowId {
                cursor_id: branch_cursor_id,
                dest: rowid_reg,
            });
        }

        // Note: For Union, we use RowSetAdd (not RowSetTest) for all branches.
        // Deduplication happens in RowSetRead via sort + dedup.
        // For Intersection, batch semantics are handled in the intersection-specific code.
        if is_intersection {
            if branch_idx == 0 {
                // First branch: just add to rowset1
                program.emit_insn(Insn::RowSetAdd {
                    rowset_reg: rowset1_reg,
                    value_reg: rowid_reg,
                });
            } else {
                // Subsequent branches: test against previous rowset
                program.emit_insn(Insn::RowSetTest {
                    rowset_reg: current_read_rowset,
                    pc_if_found: found_in_prev_label.unwrap(),
                    value_reg: rowid_reg,
                    batch: -1, // Test only, no insert
                });
                // Not found - skip to next
                program.emit_insn(Insn::Goto {
                    target_pc: branch_next,
                });
                // Found - add to result rowset
                program.preassign_label_to_next_insn(found_in_prev_label.unwrap());
                program.emit_insn(Insn::RowSetAdd {
                    rowset_reg: current_write_rowset,
                    value_reg: rowid_reg,
                });
            }
        } else {
            // Union: Just add all rowids, RowSetRead will deduplicate
            program.emit_insn(Insn::RowSetAdd {
                rowset_reg: rowset1_reg,
                value_reg: rowid_reg,
            });
        }

        program.preassign_label_to_next_insn(branch_next);
        match seek_def.iter_dir {
            IterationDirection::Forwards => program.emit_insn(Insn::Next {
                cursor_id: branch_cursor_id,
                pc_if_next: branch_loop_start,
            }),
            IterationDirection::Backwards => program.emit_insn(Insn::Prev {
                cursor_id: branch_cursor_id,
                pc_if_prev: branch_loop_start,
            }),
        }
        program.preassign_label_to_next_insn(branch_loop_end);

        // For intersection with more than 2 branches, swap the rowsets
        // so the next iteration reads from what we just wrote and writes to the other
        if is_intersection && branch_idx > 0 && branch_idx < multi_idx_op.branches.len() - 1 {
            std::mem::swap(&mut current_read_rowset, &mut current_write_rowset);
            // Re-initialize the write rowset for the next iteration
            program.emit_insn(Insn::Null {
                dest: current_write_rowset,
                dest_end: None,
            });
        }
    }

    // Determine which rowset to read from for final results
    let final_rowset = if is_intersection && multi_idx_op.branches.len() > 1 {
        // For intersection:
        // - Branch 0: writes to rowset1 (rowset1 is write-only, never tested)
        // - Branch 1: tests against rowset1, writes matches to rowset2
        // - Branch 2: tests against rowset2, writes matches to rowset1 (after swap)
        // - etc.
        //
        // After N branches, the results are in the rowset that branch N-1 wrote to.
        // Branch 0 writes to rowset1, branch 1 writes to rowset2, branch 2 writes to rowset1...
        // Number of swaps = N - 2 (swaps happen after branches 1, 2, ... N-2)
        //
        // For 2 branches: 0 swaps, final is rowset2 (branch 1 wrote there)
        // For 3 branches: 1 swap, final is rowset1 (branch 2 wrote there after swap)
        // For 4 branches: 2 swaps, final is rowset2
        //
        // Pattern: N branches -> (N-2) swaps
        // If (N-2) is even -> rowset2, if odd -> rowset1
        let num_swaps = multi_idx_op.branches.len().saturating_sub(2);
        if num_swaps % 2 == 0 {
            rowset2_reg
        } else {
            rowset1_reg
        }
    } else {
        rowset1_reg
    };

    // Now read from RowSet and fetch actual rows
    program.preassign_label_to_next_insn(loop_start);
    program.emit_insn(Insn::RowSetRead {
        rowset_reg: final_rowset,
        pc_if_empty: loop_end,
        dest_reg: rowid_reg,
    });

    // Seek to the row in the table
    let skip_label = program.allocate_label();
    program.emit_insn(Insn::SeekRowid {
        cursor_id: table_cursor_id,
        src_reg: rowid_reg,
        target_pc: skip_label,
    });

    // Cache the rowid expression for later use
    let rowid_expr = Expr::RowId {
        database: None,
        table: table.internal_id,
    };
    t_ctx
        .resolver
        .expr_to_reg_cache
        .push((Cow::Owned(rowid_expr), rowid_reg));

    program.preassign_label_to_next_insn(skip_label);

    Ok(())
}

/// Set up the main query execution loop
/// For example in the case of a nested table scan, this means emitting the Rewind instruction
/// for all tables involved, outermost first.
#[allow(clippy::too_many_arguments)]
pub fn open_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &TableReferences,
    join_order: &[JoinOrderMember],
    predicates: &[WhereTerm],
    temp_cursor_id: Option<CursorID>,
    mode: OperationMode,
    subqueries: &mut [NonFromClauseSubquery],
) -> Result<()> {
    let live_table_ids: HashSet<_> = join_order.iter().map(|member| member.table_id).collect();
    for (join_index, join) in join_order.iter().enumerate() {
        let joined_table_index = join.original_idx;
        let table = &table_references.joined_tables()[joined_table_index];
        let LoopLabels {
            loop_start,
            loop_end,
            next,
        } = *t_ctx
            .labels_main_loop
            .get(joined_table_index)
            .expect("table has no loop labels");

        // Each OUTER JOIN has a "match flag" that is initially set to false,
        // and is set to true when a match is found for the OUTER JOIN.
        // This is used to determine whether to emit actual columns or NULLs for the columns of the right table.
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_meta = t_ctx.meta_left_joins[joined_table_index].as_ref().unwrap();
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: lj_meta.reg_match_flag,
                });
            }
        }

        let (table_cursor_id, index_cursor_id) = table.resolve_cursors(program, mode.clone())?;

        match &table.op {
            Operation::Scan(scan) => {
                match (scan, &table.table) {
                    (Scan::BTreeTable { iter_dir, .. }, Table::BTree(_)) => {
                        let iteration_cursor_id = temp_cursor_id.unwrap_or_else(|| {
                            index_cursor_id.unwrap_or_else(|| {
                                table_cursor_id.expect(
                                    "Either ephemeral or index or table cursor must be opened",
                                )
                            })
                        });
                        if *iter_dir == IterationDirection::Backwards {
                            program.emit_insn(Insn::Last {
                                cursor_id: iteration_cursor_id,
                                pc_if_empty: loop_end,
                            });
                        } else {
                            program.emit_insn(Insn::Rewind {
                                cursor_id: iteration_cursor_id,
                                pc_if_empty: loop_end,
                            });
                        }
                        program.preassign_label_to_next_insn(loop_start);
                    }
                    (
                        Scan::VirtualTable {
                            idx_num,
                            idx_str,
                            constraints,
                        },
                        Table::Virtual(_),
                    ) => {
                        let (start_reg, count, maybe_idx_str, maybe_idx_int) = {
                            let args_needed = constraints.len();
                            let start_reg = program.alloc_registers(args_needed);

                            for (argv_index, expr) in constraints.iter().enumerate() {
                                let target_reg = start_reg + argv_index;
                                translate_expr(
                                    program,
                                    Some(table_references),
                                    expr,
                                    target_reg,
                                    &t_ctx.resolver,
                                )?;
                            }

                            // If best_index provided an idx_str, translate it.
                            let maybe_idx_str = if let Some(idx_str) = idx_str {
                                let reg = program.alloc_register();
                                program.emit_insn(Insn::String8 {
                                    dest: reg,
                                    value: idx_str.to_owned(),
                                });
                                Some(reg)
                            } else {
                                None
                            };
                            (start_reg, args_needed, maybe_idx_str, Some(*idx_num))
                        };

                        // Emit VFilter with the computed arguments.
                        program.emit_insn(Insn::VFilter {
                            cursor_id: table_cursor_id
                                .expect("Virtual tables do not support covering indexes"),
                            arg_count: count,
                            args_reg: start_reg,
                            idx_str: maybe_idx_str,
                            idx_num: maybe_idx_int.unwrap_or(0) as usize,
                            pc_if_empty: loop_end,
                        });
                        program.preassign_label_to_next_insn(loop_start);
                    }
                    (Scan::Subquery, Table::FromClauseSubquery(from_clause_subquery)) => {
                        match from_clause_subquery.plan.select_query_destination() {
                            Some(QueryDestination::CoroutineYield {
                                yield_reg,
                                coroutine_implementation_start,
                            }) => {
                                // Coroutine-based subquery execution
                                // In case the subquery is an inner loop, it needs to be reinitialized on each iteration of the outer loop.
                                program.emit_insn(Insn::InitCoroutine {
                                    yield_reg: *yield_reg,
                                    jump_on_definition: BranchOffset::Offset(0),
                                    start_offset: *coroutine_implementation_start,
                                });
                                program.preassign_label_to_next_insn(loop_start);
                                // A subquery within the main loop of a parent query has no cursor, so instead of advancing the cursor,
                                // it emits a Yield which jumps back to the main loop of the subquery itself to retrieve the next row.
                                // When the subquery coroutine completes, this instruction jumps to the label at the top of the termination_label_stack,
                                // which in this case is the end of the Yield-Goto loop in the parent query.
                                program.emit_insn(Insn::Yield {
                                    yield_reg: *yield_reg,
                                    end_offset: loop_end,
                                });
                            }
                            Some(QueryDestination::EphemeralTable { cursor_id, .. }) => {
                                // Materialized CTE - scan the ephemeral table with Rewind/Next
                                program.emit_insn(Insn::Rewind {
                                    cursor_id: *cursor_id,
                                    pc_if_empty: loop_end,
                                });
                                program.preassign_label_to_next_insn(loop_start);
                                // Emit Column instructions to read from the ephemeral table
                                // into the result registers at the start of each iteration
                                if let Some(start_reg) =
                                    from_clause_subquery.result_columns_start_reg
                                {
                                    for col_idx in 0..from_clause_subquery.columns.len() {
                                        program.emit_insn(Insn::Column {
                                            cursor_id: *cursor_id,
                                            column: col_idx,
                                            dest: start_reg + col_idx,
                                            default: None,
                                        });
                                    }
                                }
                            }
                            _ => unreachable!("Subquery table with unexpected query destination"),
                        }
                    }
                    _ => unreachable!(
                        "{:?} scan cannot be used with {:?} table",
                        scan, table.table
                    ),
                }
                if let Some(table_cursor_id) = table_cursor_id {
                    if let Some(index_cursor_id) = index_cursor_id {
                        program.emit_insn(Insn::DeferredSeek {
                            index_cursor_id,
                            table_cursor_id,
                        });
                    }
                }
            }
            Operation::Search(search) => {
                // Check if this is a FROM clause subquery with a materialized ephemeral index
                let is_materialized_subquery = matches!(&table.table, Table::FromClauseSubquery(_))
                    && matches!(search, Search::Seek { index: Some(idx), .. } if idx.ephemeral);

                // Open the loop for the index search.
                // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, since it is a single row lookup.
                match search {
                    Search::RowidEq { cmp_expr } => {
                        assert!(
                            !matches!(table.table, Table::FromClauseSubquery(_)),
                            "Subqueries do not support rowid seeks"
                        );
                        let src_reg = program.alloc_register();
                        translate_expr(
                            program,
                            Some(table_references),
                            cmp_expr,
                            src_reg,
                            &t_ctx.resolver,
                        )?;
                        program.emit_insn(Insn::SeekRowid {
                            cursor_id: table_cursor_id
                                .expect("Search::RowidEq requires a table cursor"),
                            src_reg,
                            target_pc: next,
                        });
                    }
                    Search::Seek {
                        index, seek_def, ..
                    } => {
                        // Otherwise, it's an index/rowid scan, i.e. first a seek is performed and then a scan until the comparison expression is not satisfied anymore.
                        let mut bloom_filter = false;
                        if let Some(index) = index {
                            if index.ephemeral && !is_materialized_subquery {
                                // For regular tables with ephemeral indexes, build the auto-index now.
                                // For materialized subqueries, the index was already built during emission.
                                let table_has_rowid = if let Table::BTree(btree) = &table.table {
                                    btree.has_rowid
                                } else {
                                    false
                                };
                                let num_seek_keys = seek_def.size(&seek_def.start);
                                let AutoIndexResult {
                                    use_bloom_filter, ..
                                } = emit_autoindex(
                                    program,
                                    index,
                                    table_cursor_id.expect(
                                        "an ephemeral index must have a source table cursor",
                                    ),
                                    index_cursor_id
                                        .expect("an ephemeral index must have an index cursor"),
                                    table_has_rowid,
                                    num_seek_keys,
                                    seek_def,
                                )?;
                                bloom_filter = use_bloom_filter;
                            }
                        }

                        // For materialized subqueries, use the index cursor directly
                        let seek_cursor_id = if is_materialized_subquery {
                            index_cursor_id.expect("materialized subquery must have index cursor")
                        } else {
                            temp_cursor_id.unwrap_or_else(|| {
                                index_cursor_id.unwrap_or_else(|| {
                                    table_cursor_id.expect(
                                        "Either ephemeral or index or table cursor must be opened",
                                    )
                                })
                            })
                        };

                        let max_registers = seek_def
                            .size(&seek_def.start)
                            .max(seek_def.size(&seek_def.end));
                        let start_reg = program.alloc_registers(max_registers);
                        emit_seek(
                            program,
                            table_references,
                            seek_def,
                            t_ctx,
                            seek_cursor_id,
                            start_reg,
                            loop_end,
                            index.as_ref(),
                            bloom_filter,
                        )?;
                        emit_seek_termination(
                            program,
                            table_references,
                            seek_def,
                            t_ctx,
                            seek_cursor_id,
                            start_reg,
                            loop_start,
                            loop_end,
                            index.as_ref(),
                        )?;

                        if is_materialized_subquery {
                            // For materialized subqueries with seek indexes, emit Column
                            // instructions to populate result registers from the covering index.
                            // The index contains all columns so we can read directly from it.
                            if let Table::FromClauseSubquery(from_clause_subquery) = &table.table {
                                if let Some(start_reg) =
                                    from_clause_subquery.result_columns_start_reg
                                {
                                    let index_cursor = index_cursor_id
                                        .expect("materialized subquery must have index cursor");
                                    for col_idx in 0..from_clause_subquery.columns.len() {
                                        program.emit_insn(Insn::Column {
                                            cursor_id: index_cursor,
                                            column: col_idx,
                                            dest: start_reg + col_idx,
                                            default: None,
                                        });
                                    }
                                }
                            }
                        } else {
                            // Only emit DeferredSeek for non-subquery tables
                            if let Some(index_cursor_id) = index_cursor_id {
                                if let Some(table_cursor_id) = table_cursor_id {
                                    // Don't do a btree table seek until it's actually necessary to read from the table.
                                    program.emit_insn(Insn::DeferredSeek {
                                        index_cursor_id,
                                        table_cursor_id,
                                    });
                                }
                            }
                        }
                    }
                }
            }
            Operation::IndexMethodQuery(query) => {
                let start_reg = program.alloc_registers(query.arguments.len() + 1);
                program.emit_int(query.pattern_idx as i64, start_reg);
                for i in 0..query.arguments.len() {
                    translate_expr(
                        program,
                        Some(table_references),
                        &query.arguments[i],
                        start_reg + 1 + i,
                        &t_ctx.resolver,
                    )?;
                }
                program.emit_insn(Insn::IndexMethodQuery {
                    db: 0,
                    cursor_id: index_cursor_id.expect("IndexMethod requires a index cursor"),
                    start_reg,
                    count_reg: query.arguments.len() + 1,
                    pc_if_empty: loop_end,
                });
                program.preassign_label_to_next_insn(loop_start);
                if let Some(table_cursor_id) = table_cursor_id {
                    if let Some(index_cursor_id) = index_cursor_id {
                        program.emit_insn(Insn::DeferredSeek {
                            index_cursor_id,
                            table_cursor_id,
                        });
                    }
                }
            }
            Operation::HashJoin(hash_join_op) => {
                // Get build table info for cursor resolution and hash table reference.
                // The optimizer convention: build=LHS, probe=RHS (the current table).
                // For outer joins, the hash table still contains the LHS rows.
                // After the probe scan, unmatched LHS entries are emitted with NULLs.
                let build_table = &table_references.joined_tables()[hash_join_op.build_table_idx];
                let (build_cursor_id, _) = build_table.resolve_cursors(program, mode.clone())?;
                let build_cursor_id = if let Some(cursor_id) = build_cursor_id {
                    cursor_id
                } else {
                    let btree = build_table
                        .btree()
                        .expect("Hash join build table must be a BTree table");
                    let cursor_id = program.alloc_cursor_id_keyed_if_not_exists(
                        CursorKey::table(build_table.internal_id),
                        CursorType::BTreeTable(btree.clone()),
                    );
                    program.emit_insn(Insn::OpenRead {
                        cursor_id,
                        root_page: btree.root_page,
                        db: build_table.database_id,
                    });
                    cursor_id
                };

                // Allocate a separate cursor for the hash build phase.
                // This keeps the build scan independent from the probe cursor.
                let hash_table_id: usize = build_table.internal_id.into();
                let btree = build_table
                    .btree()
                    .expect("Hash join build table must be a BTree table");
                let hash_build_cursor_id = program.alloc_cursor_id_keyed_if_not_exists(
                    CursorKey::hash_build(build_table.internal_id),
                    CursorType::BTreeTable(btree.clone()),
                );
                let payload_info = emit_hash_build_phase(
                    program,
                    t_ctx,
                    table_references,
                    subqueries,
                    predicates,
                    hash_join_op,
                    hash_build_cursor_id,
                    hash_table_id,
                )?;

                let num_keys = hash_join_op.join_keys.len();

                // Hash Table Probe Phase
                // Iterate through probe table and look up matches in hash table
                let probe_cursor_id = table_cursor_id.expect("Probe table must have a cursor");
                program.emit_insn(Insn::Rewind {
                    cursor_id: probe_cursor_id,
                    pc_if_empty: loop_end,
                });

                program.preassign_label_to_next_insn(loop_start);

                // Translate probe key expressions into registers
                let probe_key_start_reg = program.alloc_registers(num_keys);
                for (idx, join_key) in hash_join_op.join_keys.iter().enumerate() {
                    let probe_expr = join_key.get_probe_expr(predicates);
                    translate_expr(
                        program,
                        Some(table_references),
                        probe_expr,
                        probe_key_start_reg + idx,
                        &t_ctx.resolver,
                    )?;
                }

                // Apply affinity conversion to probe keys before hashing to match build keys
                if let Some(count) = std::num::NonZeroUsize::new(num_keys) {
                    program.emit_insn(Insn::Affinity {
                        start_reg: probe_key_start_reg,
                        count,
                        affinities: payload_info.key_affinities.clone(),
                    });
                }

                if payload_info.use_bloom_filter {
                    program.emit_insn(Insn::Filter {
                        cursor_id: payload_info.bloom_filter_cursor_id,
                        target_pc: next,
                        key_reg: probe_key_start_reg,
                        num_keys,
                    });
                }

                // Allocate payload destination registers if we have payload columns
                let num_payload = payload_info.payload_columns.len();
                let payload_dest_reg = if num_payload > 0 {
                    Some(program.alloc_registers(num_payload))
                } else {
                    None
                };

                // For FULL OUTER: initialize match flag per probe row.
                // When a probe row has no build match, we emit probe + NULLs for build.
                // For LEFT OUTER: not needed (unmatched probe rows are just skipped).
                if matches!(hash_join_op.join_type, HashJoinType::FullOuter) {
                    let probe_table_idx = hash_join_op.probe_table_idx;
                    if let Some(lj_meta) = t_ctx.meta_left_joins[probe_table_idx].as_ref() {
                        program.emit_insn(Insn::Integer {
                            value: 0,
                            dest: lj_meta.reg_match_flag,
                        });
                    }
                }

                // For FULL OUTER, probe miss → check_outer (emit probe + NULLs for build).
                // For LEFT OUTER, probe miss → next (skip, unmatched probe rows don't appear).
                // For inner, probe miss -> next.
                let hash_probe_miss_label = if hash_join_op.join_type == HashJoinType::FullOuter {
                    // This label will be resolved in close_loop via hash_ctx
                    program.allocate_label()
                } else {
                    next
                };

                // Probe hash table with keys, store matched rowid and payload in registers
                let match_reg = program.alloc_register();
                program.emit_insn(Insn::HashProbe {
                    hash_table_id: to_u16(hash_table_id),
                    key_start_reg: to_u16(probe_key_start_reg),
                    num_keys: to_u16(num_keys),
                    dest_reg: to_u16(match_reg),
                    target_pc: hash_probe_miss_label,
                    payload_dest_reg: payload_dest_reg.map(to_u16),
                    num_payload: to_u16(num_payload),
                });

                // Label for match processing, HashNext jumps here to avoid re-probing
                let match_found_label = program.allocate_label();
                program.preassign_label_to_next_insn(match_found_label);
                let hash_next_label = program.allocate_label();

                // Store hash context for later use
                let payload_columns = payload_info.payload_columns.clone();
                t_ctx.hash_table_contexts.insert(
                    hash_join_op.build_table_idx,
                    HashCtx {
                        hash_table_reg: hash_table_id,
                        match_reg,
                        match_found_label,
                        hash_next_label,
                        payload_start_reg: payload_dest_reg,
                        payload_columns: payload_info.payload_columns,
                        check_outer_label: if hash_join_op.join_type == HashJoinType::FullOuter {
                            Some(hash_probe_miss_label)
                        } else {
                            None
                        },
                        build_cursor_id: Some(build_cursor_id),
                        join_type: hash_join_op.join_type,
                    },
                );

                // Add payload columns to resolver's cache so translate_expr will
                // use Copy from payload registers instead of reading from cursor.
                t_ctx.resolver.enable_expr_to_reg_cache();
                let rowid_expr = Expr::RowId {
                    database: None,
                    table: build_table.internal_id,
                };
                let payload_has_build_rowid = payload_columns.iter().any(|payload| {
                    matches!(
                        payload,
                        MaterializedColumnRef::RowId { table_id } if *table_id == build_table.internal_id
                    )
                });
                let build_table_is_live = live_table_ids.contains(&build_table.internal_id);
                if payload_info.allow_seek && !payload_has_build_rowid && !build_table_is_live {
                    t_ctx
                        .resolver
                        .expr_to_reg_cache
                        .push((Cow::Owned(rowid_expr), match_reg));
                }
                if let Some(payload_reg) = payload_dest_reg {
                    for (i, payload) in payload_columns.iter().enumerate() {
                        let (payload_table_id, expr) = match payload {
                            MaterializedColumnRef::Column {
                                table_id,
                                column_idx,
                                is_rowid_alias,
                            } => (
                                *table_id,
                                Expr::Column {
                                    database: None,
                                    table: *table_id,
                                    column: *column_idx,
                                    is_rowid_alias: *is_rowid_alias,
                                },
                            ),
                            MaterializedColumnRef::RowId { table_id } => (
                                *table_id,
                                Expr::RowId {
                                    database: None,
                                    table: *table_id,
                                },
                            ),
                        };
                        if live_table_ids.contains(&payload_table_id) {
                            continue;
                        }
                        t_ctx
                            .resolver
                            .expr_to_reg_cache
                            .push((Cow::Owned(expr), payload_reg + i));
                    }
                } else if payload_info.allow_seek && !build_table_is_live {
                    // When payload doesn't contain all needed columns, SeekRowid to the build table.
                    program.emit_insn(Insn::SeekRowid {
                        cursor_id: build_cursor_id,
                        src_reg: match_reg,
                        target_pc: hash_next_label,
                    });
                }
            }
            Operation::MultiIndexScan(multi_idx_op) => {
                emit_multi_index_scan_loop(
                    program,
                    t_ctx,
                    table,
                    table_references,
                    multi_idx_op,
                    loop_start,
                    loop_end,
                    next,
                )?;
            }
        }

        let condition_fail_target = if let Operation::HashJoin(ref hj) = table.op {
            t_ctx
                .hash_table_contexts
                .get(&hj.build_table_idx)
                .map(|ctx| ctx.hash_next_label)
                .expect("should have hash context for build table")
        } else {
            next
        };

        // First emit outer join conditions, if any.
        emit_conditions(
            program,
            &t_ctx,
            table_references,
            join_order,
            predicates,
            join_index,
            condition_fail_target,
            true,
            subqueries,
            SubqueryRefFilter::All,
        )?;

        // Set the match flag to true if this is a LEFT JOIN.
        // At this point of execution we are going to emit columns for the left table,
        // and either emit columns or NULLs for the right table, depending on whether the null_flag is set
        // for the right table's cursor.
        // Skip for tables that are the probe side of an outer hash join — their outer
        // semantics are handled by the hash join's unmatched build scan instead.
        let is_outer_hj_probe = matches!(table.op, Operation::HashJoin(ref hj) if matches!(
            hj.join_type,
            HashJoinType::LeftOuter | HashJoinType::FullOuter
        ));
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer && !is_outer_hj_probe {
                let lj_meta = t_ctx.meta_left_joins[joined_table_index].as_ref().unwrap();
                program.resolve_label(lj_meta.label_match_flag_set_true, program.offset());
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: lj_meta.reg_match_flag,
                });
            }
        }

        // For outer hash join probes: on match, mark the hash table entry as matched
        // and set the match flag for FULL OUTER per-probe-row tracking.
        if let Operation::HashJoin(ref hj) = table.op {
            if matches!(
                hj.join_type,
                HashJoinType::LeftOuter | HashJoinType::FullOuter
            ) {
                let build_table = &table_references.joined_tables()[hj.build_table_idx];
                let hash_table_id: usize = build_table.internal_id.into();
                // Mark this hash table entry as matched (for unmatched build scan later)
                program.emit_insn(Insn::HashMarkMatched { hash_table_id });

                // For FULL OUTER: also set match flag to 1 (this probe row found a match)
                if matches!(hj.join_type, HashJoinType::FullOuter) {
                    let probe_idx = hj.probe_table_idx;
                    if let Some(lj_meta) = t_ctx.meta_left_joins[probe_idx].as_ref() {
                        program.resolve_label(lj_meta.label_match_flag_set_true, program.offset());
                        program.emit_insn(Insn::Integer {
                            value: 1,
                            dest: lj_meta.reg_match_flag,
                        });
                    }
                }
            }
        }

        // emit conditions that do not reference subquery results
        emit_conditions(
            program,
            &t_ctx,
            table_references,
            join_order,
            predicates,
            join_index,
            condition_fail_target,
            false,
            subqueries,
            SubqueryRefFilter::WithoutSubqueryRefs,
        )?;

        for subquery in subqueries.iter_mut().filter(|s| !s.has_been_evaluated()) {
            turso_assert!(subquery.correlated, "subquery must be correlated");
            let eval_at = subquery.get_eval_at(join_order, Some(table_references))?;

            if eval_at != EvalAt::Loop(join_index) {
                continue;
            }

            let plan = subquery.consume_plan(eval_at);

            emit_non_from_clause_subquery(
                program,
                &t_ctx.resolver,
                *plan,
                &subquery.query_type,
                subquery.correlated,
            )?;
        }

        // FINALLY emit conditions that DO reference subquery results.
        // These depend on the subquery evaluation that just happened above.
        emit_conditions(
            program,
            &t_ctx,
            table_references,
            join_order,
            predicates,
            join_index,
            condition_fail_target,
            false,
            subqueries,
            SubqueryRefFilter::WithSubqueryRefs,
        )?;
    }

    if subqueries.iter().any(|s| !s.has_been_evaluated()) {
        crate::bail_parse_error!(
            "all subqueries should have already been emitted, but found {} unevaluated subqueries",
            subqueries
                .iter()
                .filter(|s| !s.has_been_evaluated())
                .count()
        );
    }

    Ok(())
}

// this is used to determine the order in which WHERE conditions should be emitted
fn condition_references_subquery(expr: &Expr, subqueries: &[NonFromClauseSubquery]) -> bool {
    let mut found = false;
    let _ = walk_expr(expr, &mut |e: &Expr| -> Result<WalkControl> {
        if let Expr::SubqueryResult { subquery_id, .. } = e {
            if subqueries.iter().any(|s| s.internal_id == *subquery_id) {
                found = true;
                return Ok(WalkControl::SkipChildren);
            }
        }
        Ok(WalkControl::Continue)
    });
    found
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SubqueryRefFilter {
    /// Emit all conditions regardless of subquery references
    All,
    /// Only emit conditions that do NOT reference subqueries (for early evaluation)
    WithoutSubqueryRefs,
    /// Only emit conditions that DO reference subqueries (for late evaluation)
    WithSubqueryRefs,
}

#[allow(clippy::too_many_arguments)]
/// Emits WHERE/ON predicates that must be evaluated at the current join loop.
fn emit_conditions(
    program: &mut ProgramBuilder,
    t_ctx: &&mut TranslateCtx,
    table_references: &TableReferences,
    join_order: &[JoinOrderMember],
    predicates: &[WhereTerm],
    join_index: usize,
    next: BranchOffset,
    from_outer_join: bool,
    subqueries: &[NonFromClauseSubquery],
    subquery_ref_filter: SubqueryRefFilter,
) -> Result<()> {
    for cond in predicates
        .iter()
        .filter(|cond| cond.from_outer_join.is_some() == from_outer_join)
        .filter(|cond| {
            cond.should_eval_at_loop(join_index, join_order, subqueries, Some(table_references))
        })
        .filter(|cond| match subquery_ref_filter {
            SubqueryRefFilter::All => true,
            SubqueryRefFilter::WithoutSubqueryRefs => {
                !condition_references_subquery(&cond.expr, subqueries)
            }
            SubqueryRefFilter::WithSubqueryRefs => {
                condition_references_subquery(&cond.expr, subqueries)
            }
        })
    {
        let jump_target_when_true = program.allocate_label();
        let condition_metadata = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true,
            jump_target_when_false: next,
            jump_target_when_null: next,
        };
        translate_condition_expr(
            program,
            table_references,
            &cond.expr,
            condition_metadata,
            &t_ctx.resolver,
        )?;
        program.preassign_label_to_next_insn(jump_target_when_true);
    }

    Ok(())
}

/// SQLite (and so Turso) processes joins as a nested loop.
/// The loop may emit rows to various destinations depending on the query:
/// - a GROUP BY sorter (grouping is done by sorting based on the GROUP BY keys and aggregating while the GROUP BY keys match)
/// - a GROUP BY phase with no sorting (when the rows are already in the order required by the GROUP BY keys)
/// - an AggStep (the columns are collected for aggregation, which is finished later)
/// - a Window (rows are buffered and returned according to the rules of the window definition)
/// - an ORDER BY sorter (when there is none of the above, but there is an ORDER BY)
/// - a QueryResult (there is none of the above, so the loop either emits a ResultRow, or if it's a subquery, yields to the parent query)
enum LoopEmitTarget {
    GroupBy,
    OrderBySorter,
    AggStep,
    Window,
    QueryResult,
}

/// Emits the bytecode for the inner loop of a query.
/// At this point the cursors for all tables have been opened and rewound.
pub fn emit_loop<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
) -> Result<()> {
    // if we have a group by, we emit a record into the group by sorter,
    // or if the rows are already sorted, we do the group by aggregation phase directly.
    let has_group_by_exprs = plan
        .group_by
        .as_ref()
        .is_some_and(|gb| !gb.exprs.is_empty());
    if has_group_by_exprs {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::GroupBy);
    }
    // if we DONT have a group by, but we have aggregates, we emit without ResultRow.
    // we also do not need to sort because we are emitting a single row.
    if !plan.aggregates.is_empty() {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::AggStep);
    }

    // Window processing is planned so that the query plan has neither GROUP BY nor aggregates.
    // If the original query contained them, they are pushed down into a subquery.
    // Rows are buffered and returned according to the rules of the window definition.
    if plan.window.is_some() {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::Window);
    }

    // if NONE of the above applies, but we have an order by, we emit a record into the order by sorter.
    if !plan.order_by.is_empty() {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::OrderBySorter);
    }
    // if we have neither, we emit a ResultRow. In that case, if we have a Limit, we handle that with DecrJumpZero.
    emit_loop_source(program, t_ctx, plan, LoopEmitTarget::QueryResult)
}

/// This is a helper function for inner_loop_emit,
/// which does a different thing depending on the emit target.
/// See the InnerLoopEmitTarget enum for more details.
fn emit_loop_source<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    plan: &'a SelectPlan,
    emit_target: LoopEmitTarget,
) -> Result<()> {
    match emit_target {
        LoopEmitTarget::GroupBy => {
            // This function either:
            // - creates a sorter for GROUP BY operations by allocating registers and translating expressions for three types of columns:
            // 1) GROUP BY columns (used as sorting keys)
            // 2) non-aggregate, non-GROUP BY columns
            // 3) aggregate function arguments
            // - or if the rows produced by the loop are already sorted in the order required by the GROUP BY keys,
            // the group by comparisons are done directly inside the main loop.
            let aggregates = &plan.aggregates;

            let GroupByMetadata {
                row_source,
                registers,
                ..
            } = t_ctx.meta_group_by.as_ref().unwrap();

            let start_reg = registers.reg_group_by_source_cols_start;
            let mut cur_reg = start_reg;

            // Collect all non-aggregate expressions in the following order:
            // 1. GROUP BY expressions. These serve as sort keys.
            // 2. Remaining non-aggregate expressions that are not in GROUP BY.
            //
            // Example:
            //   SELECT col1, col2, SUM(col3) FROM table GROUP BY col1
            //   - col1 is added first (from GROUP BY)
            //   - col2 is added second (non-aggregate, in SELECT, not in GROUP BY)
            for (expr, _) in t_ctx.non_aggregate_expressions.iter() {
                let key_reg = cur_reg;
                cur_reg += 1;
                translate_expr(
                    program,
                    Some(&plan.table_references),
                    expr,
                    key_reg,
                    &t_ctx.resolver,
                )?;
            }

            // Step 2: Process arguments for all aggregate functions
            // For each aggregate, translate all its argument expressions
            for agg in aggregates.iter() {
                // For a query like: SELECT group_col, SUM(val1), AVG(val2) FROM table GROUP BY group_col
                // we'll process val1 and val2 here, storing them in the sorter so they're available
                // when computing the aggregates after sorting by group_col
                for expr in agg.args.iter() {
                    let agg_reg = cur_reg;
                    cur_reg += 1;
                    translate_expr(
                        program,
                        Some(&plan.table_references),
                        expr,
                        agg_reg,
                        &t_ctx.resolver,
                    )?;
                }
            }

            match row_source {
                GroupByRowSource::Sorter {
                    sort_cursor,
                    sorter_column_count,
                    reg_sorter_key,
                    ..
                } => {
                    sorter_insert(
                        program,
                        start_reg,
                        *sorter_column_count,
                        *sort_cursor,
                        *reg_sorter_key,
                    );
                }
                GroupByRowSource::MainLoop { .. } => group_by_agg_phase(program, t_ctx, plan)?,
            }

            Ok(())
        }
        LoopEmitTarget::OrderBySorter => {
            order_by_sorter_insert(program, t_ctx, plan)?;

            if let Distinctness::Distinct { ctx } = &plan.distinctness {
                let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
                program.preassign_label_to_next_insn(distinct_ctx.label_on_conflict);
            }

            Ok(())
        }
        LoopEmitTarget::AggStep => {
            let start_reg = t_ctx
                .reg_agg_start
                .expect("aggregate registers must be initialized");

            // In planner.rs, we have collected all aggregates from the SELECT clause, including ones where the aggregate is embedded inside
            // a more complex expression. Some examples: length(sum(x)), sum(x) + avg(y), sum(x) + 1, etc.
            // The result of those more complex expressions depends on the final result of the aggregate, so we don't translate the complete expressions here.
            // Instead, we accumulate the intermediate results of all aggreagates, and evaluate any expressions that do not contain aggregates.
            for (i, agg) in plan.aggregates.iter().enumerate() {
                let reg = start_reg + i;
                translate_aggregation_step(
                    program,
                    &plan.table_references,
                    AggArgumentSource::new_from_expression(&agg.func, &agg.args, &agg.distinctness),
                    reg,
                    &t_ctx.resolver,
                )?;
                if let Distinctness::Distinct { ctx } = &agg.distinctness {
                    let ctx = ctx
                        .as_ref()
                        .expect("distinct aggregate context not populated");
                    program.preassign_label_to_next_insn(ctx.label_on_conflict);
                }
            }

            let label_emit_nonagg_only_once = if let Some(flag) = t_ctx.reg_nonagg_emit_once_flag {
                let if_label = program.allocate_label();
                program.emit_insn(Insn::If {
                    reg: flag,
                    target_pc: if_label,
                    jump_if_null: false,
                });
                Some(if_label)
            } else {
                None
            };

            let col_start = t_ctx.reg_result_cols_start.unwrap();

            // Process only non-aggregate columns
            let non_agg_columns = plan
                .result_columns
                .iter()
                .enumerate()
                .filter(|(_, rc)| !rc.contains_aggregates);

            for (i, rc) in non_agg_columns {
                let reg = col_start + i;

                translate_expr(
                    program,
                    Some(&plan.table_references),
                    &rc.expr,
                    reg,
                    &t_ctx.resolver,
                )?;
            }

            // For result columns that contain aggregates but also reference
            // non-aggregate columns (e.g. CASE WHEN SUM(1) THEN a ELSE b END),
            // pre-read those column references while the cursor is still valid.
            // They are cached in expr_to_reg_cache so that when the full
            // expression is evaluated after AggFinal, translate_expr finds
            // the cached values instead of reading from the exhausted cursor.
            for rc in plan
                .result_columns
                .iter()
                .filter(|rc| rc.contains_aggregates)
            {
                walk_expr(&rc.expr, &mut |expr: &'a Expr| -> Result<WalkControl> {
                    match expr {
                        Expr::Column { .. } | Expr::RowId { .. } => {
                            let reg = program.alloc_register();
                            translate_expr(
                                program,
                                Some(&plan.table_references),
                                expr,
                                reg,
                                &t_ctx.resolver,
                            )?;
                            t_ctx
                                .resolver
                                .expr_to_reg_cache
                                .push((Cow::Borrowed(expr), reg));
                            Ok(WalkControl::SkipChildren)
                        }
                        _ => {
                            if plan.aggregates.iter().any(|a| a.original_expr == *expr) {
                                return Ok(WalkControl::SkipChildren);
                            }
                            Ok(WalkControl::Continue)
                        }
                    }
                })?;
            }

            if let Some(label) = label_emit_nonagg_only_once {
                program.resolve_label(label, program.offset());
                let flag = t_ctx.reg_nonagg_emit_once_flag.unwrap();
                program.emit_int(1, flag);
            }

            Ok(())
        }
        LoopEmitTarget::QueryResult => {
            turso_assert!(
                plan.aggregates.is_empty(),
                "QueryResult target should not have aggregates"
            );
            let offset_jump_to = t_ctx
                .labels_main_loop
                .first()
                .map(|l| l.next)
                .or(t_ctx.label_main_loop_end);
            emit_select_result(
                program,
                &t_ctx.resolver,
                plan,
                t_ctx.label_main_loop_end,
                offset_jump_to,
                t_ctx.reg_nonagg_emit_once_flag,
                t_ctx.reg_offset,
                t_ctx.reg_result_cols_start.unwrap(),
                t_ctx.limit_ctx,
            )?;

            if let Distinctness::Distinct { ctx } = &plan.distinctness {
                let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
                program.preassign_label_to_next_insn(distinct_ctx.label_on_conflict);
            }

            Ok(())
        }
        LoopEmitTarget::Window => {
            emit_window_loop_source(program, t_ctx, plan)?;

            Ok(())
        }
    }
}

/// Closes the loop for a given source operator.
/// For example in the case of a nested table scan, this means emitting the Next instruction
/// for all tables involved, innermost first.
pub fn close_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &TableReferences,
    join_order: &[JoinOrderMember],
    mode: OperationMode,
    select_plan: Option<&SelectPlan>,
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

        let (table_cursor_id, index_cursor_id) = table.resolve_cursors(program, mode.clone())?;
        match &table.op {
            Operation::Scan(scan) => {
                program.resolve_label(loop_labels.next, program.offset());
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
                        if *iter_dir == IterationDirection::Backwards {
                            program.emit_insn(Insn::Prev {
                                cursor_id: iteration_cursor_id,
                                pc_if_prev: loop_labels.loop_start,
                            });
                        } else {
                            program.emit_insn(Insn::Next {
                                cursor_id: iteration_cursor_id,
                                pc_if_next: loop_labels.loop_start,
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
                    Scan::Subquery => {
                        // Check if this is a materialized CTE (EphemeralTable) or coroutine
                        if let Table::FromClauseSubquery(subquery) = &table.table {
                            if let Some(QueryDestination::EphemeralTable { cursor_id, .. }) =
                                subquery.plan.select_query_destination()
                            {
                                // Materialized CTE - use Next to iterate
                                program.emit_insn(Insn::Next {
                                    cursor_id: *cursor_id,
                                    pc_if_next: loop_labels.loop_start,
                                });
                            } else {
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
                }
                program.preassign_label_to_next_insn(loop_labels.loop_end);
            }
            Operation::Search(search) => {
                // Materialized subqueries with ephemeral indexes are allowed
                let is_materialized_subquery = matches!(&table.table, Table::FromClauseSubquery(_))
                    && matches!(search, Search::Seek { index: Some(idx), .. } if idx.ephemeral);
                turso_assert_some!(
                    {
                        is_from_clause: !matches!(table.table, Table::FromClauseSubquery(_)),
                        is_materialized_subquery: is_materialized_subquery
                    },
                    "Subqueries do not support index seeks unless materialized"
                );
                program.resolve_label(loop_labels.next, program.offset());
                let iteration_cursor_id =
                    if let OperationMode::UPDATE(UpdateRowSource::PrebuiltEphemeralTable {
                        ephemeral_table_cursor_id,
                        ..
                    }) = &mode
                    {
                        *ephemeral_table_cursor_id
                    } else if is_materialized_subquery {
                        // For materialized subqueries, use the index cursor
                        index_cursor_id.expect("materialized subquery must have index cursor")
                    } else {
                        index_cursor_id.unwrap_or_else(|| {
                            table_cursor_id
                                .expect("Either ephemeral or index or table cursor must be opened")
                        })
                    };
                // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, so there is no need to emit a Next instruction.
                if !matches!(search, Search::RowidEq { .. }) {
                    let iter_dir = match search {
                        Search::Seek { seek_def, .. } => seek_def.iter_dir,
                        Search::RowidEq { .. } => unreachable!(),
                    };

                    if iter_dir == IterationDirection::Backwards {
                        program.emit_insn(Insn::Prev {
                            cursor_id: iteration_cursor_id,
                            pc_if_prev: loop_labels.loop_start,
                        });
                    } else {
                        program.emit_insn(Insn::Next {
                            cursor_id: iteration_cursor_id,
                            pc_if_next: loop_labels.loop_start,
                        });
                    }
                }
                program.preassign_label_to_next_insn(loop_labels.loop_end);
            }
            Operation::IndexMethodQuery(_) => {
                program.resolve_label(loop_labels.next, program.offset());
                program.emit_insn(Insn::Next {
                    cursor_id: index_cursor_id.unwrap(),
                    pc_if_next: loop_labels.loop_start,
                });
                program.preassign_label_to_next_insn(loop_labels.loop_end);
            }
            Operation::HashJoin(ref hash_join_op) => {
                // Probe table: emit logic for iterating through hash matches
                if let Some(hash_ctx) = t_ctx.hash_table_contexts.get(&hash_join_op.build_table_idx)
                {
                    let hash_table_reg = hash_ctx.hash_table_reg;
                    let match_reg = hash_ctx.match_reg;
                    let match_found_label = hash_ctx.match_found_label;
                    let hash_next_label = hash_ctx.hash_next_label;
                    let payload_dest_reg = hash_ctx.payload_start_reg;
                    let num_payload = hash_ctx.payload_columns.len();
                    let check_outer_label = hash_ctx.check_outer_label;
                    let build_cursor_id = hash_ctx.build_cursor_id;
                    let join_type = hash_ctx.join_type;
                    let label_next_probe_row = program.allocate_label();

                    // For FULL OUTER, HashNext exhaustion goes to check_outer.
                    // For LEFT OUTER and inner, it goes to next_probe_row.
                    let hash_next_target = if join_type == HashJoinType::FullOuter {
                        check_outer_label.unwrap_or(label_next_probe_row)
                    } else {
                        label_next_probe_row
                    };

                    // Resolve hash_next_label here, this is where conditions jump when they fail
                    // to try the next hash match before moving to the next probe row.
                    program.resolve_label(hash_next_label, program.offset());

                    // Check for additional matches with same probe keys
                    // If found: store in match_reg (and payload if available) and continue
                    // If not found: jump to check_outer (outer) or next_probe_row (inner)
                    program.emit_insn(Insn::HashNext {
                        hash_table_id: hash_table_reg,
                        dest_reg: match_reg,
                        target_pc: hash_next_target,
                        payload_dest_reg,
                        num_payload,
                    });

                    // Jump to match processing, skips HashProbe to preserve iteration state.
                    program.emit_insn(Insn::Goto {
                        target_pc: match_found_label,
                    });

                    // For FULL OUTER, emit the check_outer block.
                    // HashProbe miss and HashNext exhaustion both jump here.
                    // For unmatched probe rows: emit probe + NULLs for build.
                    if matches!(join_type, HashJoinType::FullOuter) {
                        // The probe table (RHS) has join_info.outer and LeftJoinMetadata
                        let probe_table_idx = hash_join_op.probe_table_idx;
                        let lj_meta = t_ctx.meta_left_joins[probe_table_idx]
                            .as_ref()
                            .expect("FULL OUTER probe table must have left join metadata");
                        let reg_match_flag = lj_meta.reg_match_flag;

                        // Resolve check_outer_label here — both HashProbe miss and
                        // HashNext exhaustion jump to this point.
                        if let Some(col) = check_outer_label {
                            program.resolve_label(col, program.offset());
                        }
                        // Also resolve the build table's label_match_flag_check_value here,
                        // since we're handling the outer join check inline instead of in the
                        // generic outer join block.
                        program
                            .resolve_label(lj_meta.label_match_flag_check_value, program.offset());

                        // If match_flag is already set (a previous hash match passed conditions),
                        // skip the NullRow emission.
                        program.emit_insn(Insn::IfPos {
                            reg: reg_match_flag,
                            target_pc: label_next_probe_row,
                            decrement_by: 0,
                        });

                        // No match — set NullRow on build cursor so Column returns NULL.
                        if let Some(cursor_id) = build_cursor_id {
                            program.emit_insn(Insn::NullRow { cursor_id });
                        }

                        // Null out payload registers if present
                        if let Some(payload_reg) = payload_dest_reg {
                            if num_payload > 0 {
                                program.emit_insn(Insn::Null {
                                    dest: payload_reg,
                                    dest_end: Some(payload_reg + num_payload - 1),
                                });
                            }
                        }

                        // Evaluate WHERE conditions (not ON conditions) on the
                        // unmatched probe row. Build columns are NULL (NullRow set).
                        // If a condition fails, skip to next probe row.
                        if let Some(plan) = select_plan {
                            for cond in plan
                                .where_clause
                                .iter()
                                .filter(|c| !c.consumed && c.from_outer_join.is_none())
                            {
                                let jump_target_when_true = program.allocate_label();
                                let condition_metadata = ConditionMetadata {
                                    jump_if_condition_is_true: false,
                                    jump_target_when_true,
                                    jump_target_when_false: label_next_probe_row,
                                    jump_target_when_null: label_next_probe_row,
                                };
                                translate_condition_expr(
                                    program,
                                    &plan.table_references,
                                    &cond.expr,
                                    condition_metadata,
                                    &t_ctx.resolver,
                                )?;
                                program.preassign_label_to_next_insn(jump_target_when_true);
                            }

                            // Emit result columns. Build cursor has NullRow set,
                            // so build columns return NULL. Use emit_loop to route
                            // through ORDER BY sorter when needed.
                            emit_loop(program, t_ctx, plan)?;
                        }
                    }

                    program.preassign_label_to_next_insn(label_next_probe_row);
                }

                // Advance to next probe row (HashProbe jumps here for inner, or after outer check)
                program.resolve_label(loop_labels.next, program.offset());
                let probe_cursor_id = table_cursor_id.expect("Probe table must have a cursor");
                program.emit_insn(Insn::Next {
                    cursor_id: probe_cursor_id,
                    pc_if_next: loop_labels.loop_start,
                });
                program.preassign_label_to_next_insn(loop_labels.loop_end);

                // For LEFT OUTER and FULL OUTER: after probe loop completes,
                // scan unmatched build entries and emit them with NULLs for the probe side.
                if matches!(
                    hash_join_op.join_type,
                    HashJoinType::LeftOuter | HashJoinType::FullOuter
                ) {
                    if let (Some(hash_ctx), Some(plan)) = (
                        t_ctx.hash_table_contexts.get(&hash_join_op.build_table_idx),
                        select_plan,
                    ) {
                        let hash_table_reg = hash_ctx.hash_table_reg;
                        let match_reg = hash_ctx.match_reg;
                        let payload_dest_reg = hash_ctx.payload_start_reg;
                        let num_payload = hash_ctx.payload_columns.len();
                        let build_cursor_id_fo = hash_ctx.build_cursor_id;

                        let done_unmatched = program.allocate_label();

                        // Set probe cursor to null mode — all probe columns return NULL
                        program.emit_insn(Insn::NullRow {
                            cursor_id: probe_cursor_id,
                        });

                        // Begin scanning unmatched build entries
                        program.emit_insn(Insn::HashScanUnmatched {
                            hash_table_id: hash_table_reg,
                            dest_reg: match_reg,
                            target_pc: done_unmatched,
                            payload_dest_reg,
                            num_payload,
                        });

                        let unmatched_loop = program.allocate_label();
                        let label_next_unmatched = program.allocate_label();
                        program.preassign_label_to_next_insn(unmatched_loop);

                        // Position build cursor to the unmatched entry's rowid
                        if let Some(cursor_id) = build_cursor_id_fo {
                            program.emit_insn(Insn::SeekRowid {
                                cursor_id,
                                src_reg: match_reg,
                                target_pc: done_unmatched,
                            });
                        }

                        // Evaluate WHERE conditions (not ON conditions) on
                        // the unmatched build row. Probe columns are NULL
                        // (NullRow set). If a condition fails, skip this row.
                        for cond in plan
                            .where_clause
                            .iter()
                            .filter(|c| !c.consumed && c.from_outer_join.is_none())
                        {
                            let jump_target_when_true = program.allocate_label();
                            let condition_metadata = ConditionMetadata {
                                jump_if_condition_is_true: false,
                                jump_target_when_true,
                                jump_target_when_false: label_next_unmatched,
                                jump_target_when_null: label_next_unmatched,
                            };
                            translate_condition_expr(
                                program,
                                &plan.table_references,
                                &cond.expr,
                                condition_metadata,
                                &t_ctx.resolver,
                            )?;
                            program.preassign_label_to_next_insn(jump_target_when_true);
                        }

                        // Emit result columns independently — probe columns return NULL
                        // (NullRow set), build columns come from the positioned build cursor.
                        // Use emit_loop to route through ORDER BY sorter when needed.
                        emit_loop(program, t_ctx, plan)?;

                        // Try the next unmatched entry
                        program.resolve_label(label_next_unmatched, program.offset());
                        program.emit_insn(Insn::HashNextUnmatched {
                            hash_table_id: hash_table_reg,
                            dest_reg: match_reg,
                            target_pc: done_unmatched,
                            payload_dest_reg,
                            num_payload,
                        });
                        program.emit_insn(Insn::Goto {
                            target_pc: unmatched_loop,
                        });

                        program.preassign_label_to_next_insn(done_unmatched);
                    }
                }
            }
            Operation::MultiIndexScan(_) => {
                // MultiIndexScan uses RowSetRead for iteration - the next is handled
                // at the end of the RowSet read loop in emit_multi_index_scan_loop
                program.resolve_label(loop_labels.next, program.offset());
                program.emit_insn(Insn::Goto {
                    target_pc: loop_labels.loop_start,
                });
                program.preassign_label_to_next_insn(loop_labels.loop_end);
            }
        }

        // Handle OUTER JOIN logic. The reason this comes after the "loop end" mark is that we may need to still jump back
        // and emit a row with NULLs for the right table, and then jump back to the next row of the left table.
        // Skip this for tables that are the probe side of an outer hash join — their outer
        // semantics are handled by the hash join's unmatched build scan.
        let is_outer_hash_join_probe = matches!(
            table.op,
            Operation::HashJoin(ref hj) if matches!(
                hj.join_type,
                HashJoinType::LeftOuter | HashJoinType::FullOuter
            )
        );
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer && !is_outer_hash_join_probe {
                let lj_meta = t_ctx.meta_left_joins[table_index].as_ref().unwrap();
                // The left join match flag is set to 1 when there is any match on the right table
                // (e.g. SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a).
                // If the left join match flag has been set to 1, we jump to the next row on the outer table,
                // i.e. continue to the next row of t1 in our example.
                program.resolve_label(lj_meta.label_match_flag_check_value, program.offset());
                let label_when_right_table_notnull = program.allocate_label();
                program.emit_insn(Insn::IfPos {
                    reg: lj_meta.reg_match_flag,
                    target_pc: label_when_right_table_notnull,
                    decrement_by: 0,
                });
                // If the left join match flag is still 0, it means there was no match on the right table,
                // but since it's a LEFT JOIN, we still need to emit a row with NULLs for the right table.
                // In that case, we now enter the routine that does exactly that.
                // First we set the right table cursor's "pseudo null bit" on, which means any Insn::Column will return NULL.
                // This needs to be set for both the table and the index cursor, if present,
                // since even if the iteration cursor is the index cursor, it might fetch values from the table cursor.
                [table_cursor_id, index_cursor_id]
                    .iter()
                    .filter_map(|maybe_cursor_id| maybe_cursor_id.as_ref())
                    .for_each(|cursor_id| {
                        program.emit_insn(Insn::NullRow {
                            cursor_id: *cursor_id,
                        });
                    });
                if let Table::FromClauseSubquery(from_clause_subquery) = &table.table {
                    if let Some(start_reg) = from_clause_subquery.result_columns_start_reg {
                        let column_count = from_clause_subquery.columns.len();
                        if column_count > 0 {
                            // Subqueries materialize their row into registers rather than being read back
                            // through a cursor. NullRow only affects cursor reads, so we also have to
                            // explicitly null out the cached registers or stale values would be re-emitted.
                            program.emit_insn(Insn::Null {
                                dest: start_reg,
                                dest_end: Some(start_reg + column_count - 1),
                            });
                        }
                    }
                }
                // Then we jump to setting the left join match flag to 1 again,
                // but this time the right table cursor will set everything to null.
                // This leads to emitting a row with cols from the left + nulls from the right,
                // and we will end up back in the IfPos instruction above, which will then
                // check the match flag again, and since it is now 1, we will jump to the
                // next row in the left table.
                program.emit_insn(Insn::Goto {
                    target_pc: lj_meta.label_match_flag_set_true,
                });
                program.preassign_label_to_next_insn(label_when_right_table_notnull);
            }
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

/// Build the affinity string for an index seek, skipping positions where the
/// probe expression already has the right type (mirroring SQLite's optimization
/// via `sqlite3ExprNeedsNoAffinityChange`).
///
/// For each seek key position the index column affinity is checked against the
/// probe expression. If the expression already matches (e.g. a string literal
/// seeking a TEXT column), NONE is used so OP_Affinity becomes a no-op for that
/// slot. When every slot is NONE the caller can skip the instruction entirely.
fn index_seek_affinities(
    idx: &Index,
    tables: &TableReferences,
    seek_def: &SeekDef,
    seek_key: &SeekKey,
) -> String {
    let table = tables
        .joined_tables()
        .iter()
        .find(|jt| jt.table.get_name() == idx.table_name)
        .expect("index source table not found in table references");

    idx.columns
        .iter()
        .zip(seek_def.iter(seek_key))
        .map(|(ic, key_component)| {
            // Expression index columns (e.g. CREATE INDEX ON t(a+b)) have no
            // corresponding table column; their affinity is BLOB/NONE.
            let col_aff = if ic.expr.is_some() {
                Affinity::Blob
            } else {
                table
                    .table
                    .get_column_at(ic.pos_in_table)
                    .expect("index column position out of bounds")
                    .affinity()
            };
            match key_component {
                SeekKeyComponent::Expr(expr) if col_aff.expr_needs_no_affinity_change(expr) => {
                    affinity::SQLITE_AFF_NONE
                }
                _ => col_aff.aff_mask(),
            }
        })
        .collect()
}

/// Emits instructions for an index seek. See e.g. [crate::translate::plan::SeekDef]
/// for more details about the seek definition.
///
/// Index seeks always position the cursor to the first row that matches the seek key,
/// and then continue to emit rows until the termination condition is reached,
/// see [emit_seek_termination] below.
///
/// If either 1. the seek finds no rows or 2. the termination condition is reached,
/// the loop for that given table/index is fully exited.
#[allow(clippy::too_many_arguments)]
fn emit_seek(
    program: &mut ProgramBuilder,
    tables: &TableReferences,
    seek_def: &SeekDef,
    t_ctx: &mut TranslateCtx,
    seek_cursor_id: usize,
    start_reg: usize,
    loop_end: BranchOffset,
    seek_index: Option<&Arc<Index>>,
    use_bloom_filter: bool,
) -> Result<()> {
    let is_index = seek_index.is_some();
    if seek_def.prefix.is_empty() && matches!(seek_def.start.last_component, SeekKeyComponent::None)
    {
        // If there is no seek key, we start from the first or last row of the index,
        // depending on the iteration direction.
        //
        // Also, if we will encounter NULLs in the index at the beginning of iteration (Forward + Asc OR Backward + Desc)
        // then, we must explicitly skip them as seek always has some bound condition over indexed column (e.g. c < ?, c >= ?, ...)
        //
        // note that table seek has some rules to convert seek key values to the integer affinity + it has some logic to check for explicit NULL searches
        // so, we emit simple Rewind/Last in case of search over table BTree
        // (this is safe as table BTree keys are always non-null single integer)
        match seek_def.iter_dir {
            IterationDirection::Forwards => {
                if seek_index.is_some_and(|index| index.columns[0].order == SortOrder::Asc) {
                    program.emit_null(start_reg, None);
                    program.emit_insn(Insn::SeekGT {
                        is_index,
                        cursor_id: seek_cursor_id,
                        start_reg,
                        num_regs: 1,
                        target_pc: loop_end,
                    });
                } else {
                    program.emit_insn(Insn::Rewind {
                        cursor_id: seek_cursor_id,
                        pc_if_empty: loop_end,
                    });
                }
            }
            IterationDirection::Backwards => {
                if seek_index.is_some_and(|index| index.columns[0].order == SortOrder::Desc) {
                    program.emit_null(start_reg, None);
                    program.emit_insn(Insn::SeekLT {
                        is_index,
                        cursor_id: seek_cursor_id,
                        start_reg,
                        num_regs: 1,
                        target_pc: loop_end,
                    });
                } else {
                    program.emit_insn(Insn::Last {
                        cursor_id: seek_cursor_id,
                        pc_if_empty: loop_end,
                    });
                }
            }
        }
        return Ok(());
    };
    // We allocated registers for the full index key, but our seek key might not use the full index key.
    // See [crate::translate::optimizer::build_seek_def] for boundary rewrites (including NULL sentinels).
    for (i, key) in seek_def.iter(&seek_def.start).enumerate() {
        let reg = start_reg + i;
        match key {
            SeekKeyComponent::Expr(expr) => {
                translate_expr_no_constant_opt(
                    program,
                    Some(tables),
                    expr,
                    reg,
                    &t_ctx.resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                // If the seek key column is not verifiably non-NULL, we need check whether it is NULL,
                // and if so, jump to the loop end.
                // This is to avoid returning rows for e.g. SELECT * FROM t WHERE t.x > NULL,
                // which would erroneously return all rows from t, as NULL is lower than any non-NULL value in index key comparisons.
                if !expr.is_nonnull(tables) {
                    program.emit_insn(Insn::IsNull {
                        reg,
                        target_pc: loop_end,
                    });
                }
            }
            SeekKeyComponent::Null => program.emit_null(reg, None),
            SeekKeyComponent::None => unreachable!("None component is not possible in iterator"),
        }
    }
    let num_regs = seek_def.size(&seek_def.start);

    if let Some(idx) = seek_index {
        let affinities = index_seek_affinities(idx, tables, seek_def, &seek_def.start);
        if affinities.chars().any(|c| c != affinity::SQLITE_AFF_NONE) {
            program.emit_insn(Insn::Affinity {
                start_reg,
                count: std::num::NonZeroUsize::new(num_regs).unwrap(),
                affinities,
            });
        }
    }
    if let Some(idx) = seek_index {
        if use_bloom_filter {
            turso_assert!(
                idx.ephemeral,
                "bloom filter can only be used with ephemeral indexes"
            );
            program.emit_insn(Insn::Filter {
                cursor_id: seek_cursor_id,
                key_reg: start_reg,
                num_keys: num_regs,
                target_pc: loop_end,
            });
        }
    }
    match seek_def.start.op {
        SeekOp::GE { eq_only } => program.emit_insn(Insn::SeekGE {
            is_index,
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
            eq_only,
        }),
        SeekOp::GT => program.emit_insn(Insn::SeekGT {
            is_index,
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        SeekOp::LE { eq_only } => program.emit_insn(Insn::SeekLE {
            is_index,
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
            eq_only,
        }),
        SeekOp::LT => program.emit_insn(Insn::SeekLT {
            is_index,
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
    };

    Ok(())
}

/// Emits instructions for an index seek termination. See e.g. [crate::translate::plan::SeekDef]
/// for more details about the seek definition.
///
/// Index seeks always position the cursor to the first row that matches the seek key
/// (see [emit_seek] above), and then continue to emit rows until the termination condition
/// (if any) is reached.
///
/// If the termination condition is not present, the cursor is fully scanned to the end.
#[allow(clippy::too_many_arguments)]
fn emit_seek_termination(
    program: &mut ProgramBuilder,
    tables: &TableReferences,
    seek_def: &SeekDef,
    t_ctx: &mut TranslateCtx,
    seek_cursor_id: usize,
    start_reg: usize,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
    seek_index: Option<&Arc<Index>>,
) -> Result<()> {
    let is_index = seek_index.is_some();
    if seek_def.prefix.is_empty() && matches!(seek_def.end.last_component, SeekKeyComponent::None) {
        program.preassign_label_to_next_insn(loop_start);
        // If we will encounter NULLs in the index at the end of iteration (Forward + Desc OR Backward + Asc)
        // then, we must explicitly stop before them as seek always has some bound condition over indexed column (e.g. c < ?, c >= ?, ...)
        match seek_def.iter_dir {
            IterationDirection::Forwards => {
                if seek_index.is_some_and(|index| index.columns[0].order == SortOrder::Desc) {
                    program.emit_null(start_reg, None);
                    program.emit_insn(Insn::IdxGE {
                        cursor_id: seek_cursor_id,
                        start_reg,
                        num_regs: 1,
                        target_pc: loop_end,
                    });
                }
            }
            IterationDirection::Backwards => {
                if seek_index.is_some_and(|index| index.columns[0].order == SortOrder::Asc) {
                    program.emit_null(start_reg, None);
                    program.emit_insn(Insn::IdxLE {
                        cursor_id: seek_cursor_id,
                        start_reg,
                        num_regs: 1,
                        target_pc: loop_end,
                    });
                }
            }
        }
        return Ok(());
    };

    // For all index key values apart from the last one, we are guaranteed to use the same values
    // as these values were emited from common prefix, so we don't need to emit them again.

    let num_regs = seek_def.size(&seek_def.end);
    let last_reg = start_reg + seek_def.prefix.len();
    match &seek_def.end.last_component {
        SeekKeyComponent::Expr(expr) => {
            translate_expr_no_constant_opt(
                program,
                Some(tables),
                expr,
                last_reg,
                &t_ctx.resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            // Apply affinity to the end key (same as we do for start key).
            // Without this, e.g. BETWEEN '15' AND '35' on a numeric column would
            // compare '35' as a string against numeric values, causing incorrect results.
            if let Some(idx) = seek_index {
                let affinities = index_seek_affinities(idx, tables, seek_def, &seek_def.end);
                if affinities.chars().any(|c| c != affinity::SQLITE_AFF_NONE) {
                    program.emit_insn(Insn::Affinity {
                        start_reg,
                        count: std::num::NonZeroUsize::new(num_regs).unwrap(),
                        affinities,
                    });
                }
            }
            // If the seek termination key expression is not verifiably non-NULL, we need to check whether it is NULL,
            // and if so, jump to the loop end.
            // This is to avoid returning rows for e.g. SELECT * FROM t WHERE t.x > NULL,
            // which would erroneously return all rows from t, as NULL is lower than any non-NULL value in index key comparisons.
            if !expr.is_nonnull(tables) {
                program.emit_insn(Insn::IsNull {
                    reg: last_reg,
                    target_pc: loop_end,
                });
            }
        }
        SeekKeyComponent::Null => program.emit_null(last_reg, None),
        SeekKeyComponent::None => {}
    }
    program.preassign_label_to_next_insn(loop_start);
    let mut rowid_reg = None;
    let mut affinity = None;
    if !is_index {
        rowid_reg = Some(program.alloc_register());
        program.emit_insn(Insn::RowId {
            cursor_id: seek_cursor_id,
            dest: rowid_reg.unwrap(),
        });

        affinity = if let Some(table_ref) = tables
            .joined_tables()
            .iter()
            .find(|t| t.columns().iter().any(|c| c.is_rowid_alias()))
        {
            if let Some(rowid_col_idx) = table_ref.columns().iter().position(|c| c.is_rowid_alias())
            {
                Some(table_ref.columns()[rowid_col_idx].affinity())
            } else {
                Some(Affinity::Numeric)
            }
        } else {
            Some(Affinity::Numeric)
        };
    }
    match (is_index, seek_def.end.op) {
        (true, SeekOp::GE { .. }) => program.emit_insn(Insn::IdxGE {
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        (true, SeekOp::GT) => program.emit_insn(Insn::IdxGT {
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        (true, SeekOp::LE { .. }) => program.emit_insn(Insn::IdxLE {
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        (true, SeekOp::LT) => program.emit_insn(Insn::IdxLT {
            cursor_id: seek_cursor_id,
            start_reg,
            num_regs,
            target_pc: loop_end,
        }),
        (false, SeekOp::GE { .. }) => program.emit_insn(Insn::Ge {
            lhs: rowid_reg.unwrap(),
            rhs: start_reg,
            target_pc: loop_end,
            flags: CmpInsFlags::default()
                .jump_if_null()
                .with_affinity(affinity.unwrap()),
            collation: program.curr_collation(),
        }),
        (false, SeekOp::GT) => program.emit_insn(Insn::Gt {
            lhs: rowid_reg.unwrap(),
            rhs: start_reg,
            target_pc: loop_end,
            flags: CmpInsFlags::default()
                .jump_if_null()
                .with_affinity(affinity.unwrap()),
            collation: program.curr_collation(),
        }),
        (false, SeekOp::LE { .. }) => program.emit_insn(Insn::Le {
            lhs: rowid_reg.unwrap(),
            rhs: start_reg,
            target_pc: loop_end,
            flags: CmpInsFlags::default()
                .jump_if_null()
                .with_affinity(affinity.unwrap()),
            collation: program.curr_collation(),
        }),
        (false, SeekOp::LT) => program.emit_insn(Insn::Lt {
            lhs: rowid_reg.unwrap(),
            rhs: start_reg,
            target_pc: loop_end,
            flags: CmpInsFlags::default()
                .jump_if_null()
                .with_affinity(affinity.unwrap()),
            collation: program.curr_collation(),
        }),
    };

    Ok(())
}

struct AutoIndexResult {
    use_bloom_filter: bool,
}

/// Open an ephemeral index cursor and build an automatic index on a table.
/// This is used as a last-resort to avoid a nested full table scan
/// Returns the cursor id of the ephemeral index cursor.
fn emit_autoindex(
    program: &mut ProgramBuilder,
    index: &Arc<Index>,
    table_cursor_id: CursorID,
    index_cursor_id: CursorID,
    table_has_rowid: bool,
    num_seek_keys: usize,
    seek_def: &SeekDef,
) -> Result<AutoIndexResult> {
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
    // Emit all columns from source table that are needed in the ephemeral index.
    // Also reserve a register for the rowid if the source table has rowids.
    let num_regs_to_reserve = index.columns.len() + table_has_rowid as usize;
    let ephemeral_cols_start_reg = program.alloc_registers(num_regs_to_reserve);
    for (i, col) in index.columns.iter().enumerate() {
        let reg = ephemeral_cols_start_reg + i;
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
        start_reg: to_u16(ephemeral_cols_start_reg),
        count: to_u16(num_regs_to_reserve),
        dest_reg: to_u16(record_reg),
        index_name: Some(index.name.clone()),
        affinity_str: None,
    });
    // Skip bloom filter for non-binary collations since it uses binary hashing.
    let use_bloom_filter = index.columns.iter().take(num_seek_keys).all(|col| {
        col.collation
            .is_none_or(|coll| matches!(coll, CollationSeq::Binary | CollationSeq::Unset))
    }) && seek_def.start.op.eq_only();
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
        unpacked_count: Some(num_regs_to_reserve as u16),
        flags: IdxInsertFlags::new().use_seek(false),
    });
    program.emit_insn(Insn::Next {
        cursor_id: table_cursor_id,
        pc_if_next: label_ephemeral_build_loop_start,
    });
    program.preassign_label_to_next_insn(label_ephemeral_build_end);
    Ok(AutoIndexResult { use_bloom_filter })
}
