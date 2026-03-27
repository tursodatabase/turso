use super::*;
use crate::schema::GeneratedType;
use crate::translate::emitter::HashLabels;
use crate::vdbe::builder::SelfTableContext;

#[derive(Debug, Clone)]
/// Payload layout metadata recorded during hash-build planning or reuse.
pub(super) struct HashBuildPayloadInfo {
    pub payload_columns: Vec<MaterializedColumnRef>,
    pub key_affinities: String,
    pub use_bloom_filter: bool,
    pub bloom_filter_cursor_id: CursorID,
    pub allow_seek: bool,
}

fn expr_references_outer_query(expr: &Expr, table_references: &TableReferences) -> bool {
    let mut has_outer_ref = false;
    let _ = walk_expr(expr, &mut |e: &Expr| -> Result<WalkControl> {
        match e {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                if table_references
                    .find_outer_query_ref_by_internal_id(*table)
                    .is_some()
                {
                    has_outer_ref = true;
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
    has_outer_ref
}

/// Static configuration for a fresh hash-table build.
struct HashBuildConfig {
    payload_columns: Vec<MaterializedColumnRef>,
    payload_signature_columns: Vec<usize>,
    key_affinities: String,
    collations: Vec<CollationSeq>,
    use_bloom_filter: bool,
    bloom_filter_cursor_id: CursorID,
    materialized_cursor_id: Option<CursorID>,
    use_materialized_keys: bool,
    allow_seek: bool,
    signature: HashBuildSignature,
}

/// Typestate entry point for hash-build planning.
///
/// Planning decides whether an existing hash build can be reused and, if not,
/// captures all configuration needed to emit a fresh build deterministically.
pub(crate) struct HashBuildPlanner<'a, 'plan> {
    program: &'a mut ProgramBuilder,
    t_ctx: &'a mut TranslateCtx<'plan>,
    table_references: &'a TableReferences,
    non_from_clause_subqueries: &'a [NonFromClauseSubquery],
    predicates: &'a [WhereTerm],
    hash_join_op: &'a HashJoinOp,
    hash_build_cursor_id: CursorID,
    hash_table_id: usize,
}

/// A planned hash build whose signature check has already completed.
pub(super) struct PreparedHashBuild<'a, 'plan> {
    planner: HashBuildPlanner<'a, 'plan>,
    config: HashBuildConfig,
}

/// Result of hash-build planning.
///
/// Reuse means the caller can immediately probe an existing compatible hash
/// table. Build means the caller must execute the prepared build before probing.
pub(super) enum HashBuildPlan<'a, 'plan> {
    Reuse(HashBuildPayloadInfo),
    Build(Box<PreparedHashBuild<'a, 'plan>>),
}

impl<'a, 'plan> HashBuildPlanner<'a, 'plan> {
    #[allow(clippy::too_many_arguments)]
    /// Capture the immutable inputs needed to decide whether to reuse or build.
    pub(super) const fn new(
        program: &'a mut ProgramBuilder,
        t_ctx: &'a mut TranslateCtx<'plan>,
        table_references: &'a TableReferences,
        non_from_clause_subqueries: &'a [NonFromClauseSubquery],
        predicates: &'a [WhereTerm],
        hash_join_op: &'a HashJoinOp,
        hash_build_cursor_id: CursorID,
        hash_table_id: usize,
    ) -> Self {
        Self {
            program,
            t_ctx,
            table_references,
            non_from_clause_subqueries,
            predicates,
            hash_join_op,
            hash_build_cursor_id,
            hash_table_id,
        }
    }

    /// Decide whether the hash table can be reused or must be rebuilt.
    pub(super) fn prepare(self) -> Result<HashBuildPlan<'a, 'plan>> {
        let materialized_input = self
            .t_ctx
            .materialized_build_inputs
            .get(&self.hash_join_op.build_table_idx);
        let materialized_cursor_id = materialized_input.map(|input| input.cursor_id);
        let num_keys = self.hash_join_op.join_keys.len();

        let mut key_affinities = String::new();
        for join_key in &self.hash_join_op.join_keys {
            let build_expr = join_key.get_build_expr(self.predicates);
            let probe_expr = join_key.get_probe_expr(self.predicates);
            let affinity =
                comparison_affinity(build_expr, probe_expr, Some(self.table_references), None);
            key_affinities.push(affinity.aff_mask());
        }

        let collations: Vec<CollationSeq> = self
            .hash_join_op
            .join_keys
            .iter()
            .map(|join_key| {
                let (original_lhs, original_rhs) = match join_key.build_side {
                    BinaryExprSide::Lhs => (
                        join_key.get_build_expr(self.predicates),
                        join_key.get_probe_expr(self.predicates),
                    ),
                    BinaryExprSide::Rhs => (
                        join_key.get_probe_expr(self.predicates),
                        join_key.get_build_expr(self.predicates),
                    ),
                };
                resolve_comparison_collseq(original_lhs, original_rhs, self.table_references)
                    .unwrap_or(CollationSeq::Binary)
            })
            .collect();

        let use_bloom_filter = self.hash_join_op.use_bloom_filter
            && collations
                .iter()
                .all(|c| matches!(c, CollationSeq::Binary | CollationSeq::Unset));

        let build_table = &self.table_references.joined_tables()[self.hash_join_op.build_table_idx];
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
                    let payload_signature_columns: Vec<usize> =
                        build_table.col_used_mask.iter().collect();
                    let payload_columns = payload_signature_columns
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
            self.hash_build_cursor_id
        };

        let join_key_indices = self
            .hash_join_op
            .join_keys
            .iter()
            .map(|key| key.where_clause_idx)
            .collect::<Vec<_>>();

        let signature = HashBuildSignature::new(
            build_table.internal_id,
            self.table_references,
            &join_key_indices,
            &payload_signature_columns,
            self.predicates,
        );

        if let Some(existing_info) = self.t_ctx.hash_build_cache.get(&signature) {
            return Ok(HashBuildPlan::Reuse(existing_info.clone()));
        }

        let config = HashBuildConfig {
            payload_columns,
            payload_signature_columns,
            key_affinities,
            collations,
            use_bloom_filter,
            bloom_filter_cursor_id,
            materialized_cursor_id,
            use_materialized_keys,
            allow_seek,
            signature,
        };

        Ok(HashBuildPlan::Build(Box::new(PreparedHashBuild {
            planner: self,
            config,
        })))
    }
}

impl<'a, 'plan> PreparedHashBuild<'a, 'plan> {
    /// Execute the fresh hash build: emit build-side instructions, cache,
    /// and return the payload info the caller needs for probing.
    pub(super) fn execute(self) -> Result<HashBuildPayloadInfo> {
        let Self { planner, config } = self;

        let HashBuildConfig {
            payload_columns,
            payload_signature_columns,
            key_affinities,
            collations,
            use_bloom_filter,
            bloom_filter_cursor_id,
            materialized_cursor_id,
            use_materialized_keys,
            allow_seek,
            signature,
        } = config;

        let hash_build_cursor_id = planner.hash_build_cursor_id;
        let hash_table_id = planner.hash_table_id;
        let hash_join_op = planner.hash_join_op;
        let build_table_idx = hash_join_op.build_table_idx;
        let program = &mut *planner.program;
        let t_ctx = &mut *planner.t_ctx;
        let table_references = planner.table_references;
        let non_from_clause_subqueries = planner.non_from_clause_subqueries;
        let predicates = planner.predicates;

        let num_keys = hash_join_op.join_keys.len();
        let num_payload = payload_columns.len();
        let record_len = num_keys + num_payload;

        program.mark_cursor_as_ephemeral(hash_build_cursor_id);
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: hash_build_cursor_id,
            num_columns: record_len,
            content_type: EphemeralTableContent::Hash,
            is_ordered: false,
        });

        let bloom_filter_reg = if use_bloom_filter {
            let bf_reg = program.alloc_register();
            program.emit_insn(Insn::Blob {
                dest: bf_reg,
                content: vec![0; 8192],
            });
            Some(bf_reg)
        } else {
            None
        };

        let hash_labels = HashLabels {
            next_label: program.allocate_label(),
            hash_loop_label: program.allocate_label(),
            found_label: program.allocate_label(),
            check_hash_label: program.allocate_label(),
        };

        let build_table = &table_references.joined_tables()[build_table_idx];

        if use_materialized_keys {
            let mat_cursor =
                materialized_cursor_id.expect("materialized input cursor is required");
            program.emit_insn(Insn::RewindEph {
                cursor_id: mat_cursor,
                pc_if_empty: hash_labels.next_label,
            });
            program.preassign_label_to_next_insn(hash_labels.hash_loop_label);

            let mat_record_reg = program.alloc_register();

            let hash_start_reg = program.alloc_registers(record_len);

            for key_idx in 0..num_keys {
                let col_idx = key_idx;
                program.emit_insn(Insn::Column {
                    cursor_id: mat_cursor,
                    column: col_idx,
                    dest: hash_start_reg + key_idx,
                });
            }
            for (payload_idx, col_ref) in payload_columns.iter().enumerate() {
                let col_idx = num_keys + payload_idx;
                let dest_reg = hash_start_reg + num_keys + payload_idx;
                match col_ref {
                    MaterializedColumnRef::Column {
                        column_idx,
                        is_rowid_alias,
                        ..
                    } => {
                        if *is_rowid_alias {
                            program.emit_insn(Insn::RowId {
                                cursor_id: mat_cursor,
                                dest: dest_reg,
                            });
                        } else {
                            program.emit_insn(Insn::Column {
                                cursor_id: mat_cursor,
                                column: col_idx,
                                dest: dest_reg,
                            });
                        }
                    }
                    MaterializedColumnRef::RowId { .. } => {
                        program.emit_insn(Insn::Column {
                            cursor_id: mat_cursor,
                            column: col_idx,
                            dest: dest_reg,
                        });
                    }
                }
            }

            program.emit_insn(Insn::MakeRecord {
                start_reg: hash_start_reg,
                count: record_len,
                dest_reg: mat_record_reg,
            });

            if let Some(bf_reg) = bloom_filter_reg {
                program.emit_insn(Insn::FilterAdd {
                    cursor_id: bloom_filter_cursor_id,
                    hash_reg: hash_start_reg,
                    num_regs: num_keys,
                    filter_reg: bf_reg,
                });
            }

            program.emit_insn(Insn::IdxInsert {
                cursor_id: hash_build_cursor_id,
                record_reg: mat_record_reg,
                key_reg: hash_start_reg,
                num_key_cols: num_keys,
                unpacked_reg: Some(hash_start_reg),
                num_cols: record_len,
            });

            program.emit_insn(Insn::NextEph {
                cursor_id: mat_cursor,
                pc_if_next: hash_labels.hash_loop_label,
            });
            program.preassign_label_to_next_insn(hash_labels.next_label);
        } else {
            let build_inner = table_references.joined_tables()[build_table_idx]
                .source
                .inner();
            let hash_start_reg = program.alloc_registers(record_len);
            let build_key_reg = hash_start_reg;
            let hash_payload_start_reg = hash_start_reg + num_keys;

            let self_table_ctx = if build_table.is_from_self_referencing_cte() {
                Some(SelfTableContext {
                    table_idx: build_table_idx,
                    coroutine_yield_reg: t_ctx
                        .coroutine_metadata_map
                        .get(&build_table_idx)
                        .map(|m| m.yield_reg),
                })
            } else {
                None
            };

            let build_cursor_id =
                emit_open_cursor(program, t_ctx, table_references, build_table_idx)?;

            let open_loop_params = OpenLoopParams {
                table_reference: build_table,
                referenced_tables: table_references,
                table_index: build_table_idx,
                iter_dir: None,
            };
            let loop_info = emit_open_loop(
                program,
                t_ctx,
                &open_loop_params,
                predicates,
                non_from_clause_subqueries,
                self_table_ctx,
            )?;

            for (key_idx, join_key) in hash_join_op.join_keys.iter().enumerate() {
                let build_expr = join_key.get_build_expr(predicates);
                translate_expr(
                    program,
                    t_ctx,
                    table_references,
                    build_expr,
                    build_key_reg + key_idx,
                    None,
                )?;
            }

            let cursor_id = match &build_inner {
                TableInner::BTreeTable(btree) => btree.cursor_id,
                TableInner::VirtualTable(vt) => vt.cursor_id,
                _ => unreachable!("build table must be a BTreeTable or VirtualTable"),
            };

            for (payload_idx, col_ref) in payload_columns.iter().enumerate() {
                let dest_reg = hash_payload_start_reg + payload_idx;
                match col_ref {
                    MaterializedColumnRef::Column {
                        column_idx,
                        is_rowid_alias,
                        ..
                    } => {
                        if *is_rowid_alias {
                            program.emit_insn(Insn::RowId {
                                cursor_id,
                                dest: dest_reg,
                            });
                        } else {
                            program.emit_insn(Insn::Column {
                                cursor_id,
                                column: *column_idx,
                                dest: dest_reg,
                            });
                        }
                    }
                    MaterializedColumnRef::RowId { .. } => {
                        program.emit_insn(Insn::RowId {
                            cursor_id,
                            dest: dest_reg,
                        });
                    }
                }
            }

            let mat_record_reg = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: hash_start_reg,
                count: record_len,
                dest_reg: mat_record_reg,
            });

            if let Some(bf_reg) = bloom_filter_reg {
                program.emit_insn(Insn::FilterAdd {
                    cursor_id: bloom_filter_cursor_id,
                    hash_reg: build_key_reg,
                    num_regs: num_keys,
                    filter_reg: bf_reg,
                });
            }

            program.emit_insn(Insn::IdxInsert {
                cursor_id: hash_build_cursor_id,
                record_reg: mat_record_reg,
                key_reg: hash_start_reg,
                num_key_cols: num_keys,
                unpacked_reg: Some(hash_start_reg),
                num_cols: record_len,
            });

            emit_close_loop(program, t_ctx, &loop_info, build_cursor_id)?;
        }

        let payload_info = HashBuildPayloadInfo {
            payload_columns,
            key_affinities,
            use_bloom_filter,
            bloom_filter_cursor_id,
            allow_seek,
        };
        t_ctx
            .hash_build_cache
            .insert(signature, payload_info.clone());

        Ok(payload_info)
    }
}

/// Emit the hash-join probe side once a hash table is available.
pub(super) fn emit_hash_join_probe(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &TableReferences,
    predicates: &[WhereTerm],
    hash_join_op: &HashJoinOp,
    hash_build_cursor_id: CursorID,
    hash_table_id: usize,
    payload_info: &HashBuildPayloadInfo,
    build_table_idx: usize,
) -> Result<HashJoinLoopInfo> {
    let num_keys = hash_join_op.join_keys.len();
    let num_payload = payload_info.payload_columns.len();
    let record_len = num_keys + num_payload;

    let hash_labels = HashLabels {
        next_label: program.allocate_label(),
        hash_loop_label: program.allocate_label(),
        found_label: program.allocate_label(),
        check_hash_label: program.allocate_label(),
    };

    let probe_key_reg = program.alloc_registers(num_keys);
    for (key_idx, join_key) in hash_join_op.join_keys.iter().enumerate() {
        let probe_expr = join_key.get_probe_expr(predicates);
        translate_expr(
            program,
            t_ctx,
            table_references,
            probe_expr,
            probe_key_reg + key_idx,
            None,
        )?;
    }

    let bloom_filter_reg = if payload_info.use_bloom_filter {
        t_ctx
            .bloom_filter_regs
            .get(&payload_info.bloom_filter_cursor_id)
            .copied()
    } else {
        None
    };

    if let Some(bf_reg) = bloom_filter_reg {
        program.emit_insn(Insn::Filter {
            cursor_id: payload_info.bloom_filter_cursor_id,
            hash_reg: probe_key_reg,
            num_regs: num_keys,
            filter_reg: bf_reg,
            pc_if_not_found: hash_labels.next_label,
        });
    }

    program.emit_insn(Insn::SeekHashKey {
        cursor_id: hash_build_cursor_id,
        hash_key_reg: probe_key_reg,
        num_fields: num_keys,
        pc_if_not_found: hash_labels.next_label,
        affinities: payload_info.key_affinities.clone(),
    });
    program.preassign_label_to_next_insn(hash_labels.check_hash_label);

    program.emit_insn(Insn::IdxGT {
        cursor_id: hash_build_cursor_id,
        start_reg: probe_key_reg,
        num_fields: num_keys,
        target_pc: hash_labels.next_label,
    });

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

    for (key_idx, collation) in collations.iter().enumerate() {
        let hash_key_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id: hash_build_cursor_id,
            column: key_idx,
            dest: hash_key_reg,
        });
        let probe_single_reg = probe_key_reg + key_idx;
        let pc_not_match = hash_labels.hash_loop_label;
        program.emit_insn(Insn::Ne {
            lhs: hash_key_reg,
            rhs: probe_single_reg,
            target_pc: pc_not_match,
            null_eq: false,
            collation: collation.clone(),
        });
    }
    program.preassign_label_to_next_insn(hash_labels.found_label);

    let hash_payload_start_reg = program.alloc_registers(num_payload);
    for payload_idx in 0..num_payload {
        let col_in_hash = num_keys + payload_idx;
        program.emit_insn(Insn::Column {
            cursor_id: hash_build_cursor_id,
            column: col_in_hash,
            dest: hash_payload_start_reg + payload_idx,
        });
    }

    let build_table = &table_references.joined_tables()[build_table_idx];

    for (payload_idx, col_ref) in payload_info.payload_columns.iter().enumerate() {
        let reg = hash_payload_start_reg + payload_idx;
        match col_ref {
            MaterializedColumnRef::Column {
                table_id,
                column_idx,
                ..
            } => {
                t_ctx
                    .resolver
                    .set_column_reg(*table_id, *column_idx, reg)?;
            }
            MaterializedColumnRef::RowId { table_id } => {
                t_ctx.resolver.set_rowid_reg(*table_id, reg)?;
            }
        }
    }

    // Recompute virtual generated columns for the build-side table, since
    // we've just loaded the base-column values from the hash payload.
    // But ONLY if we are NOT reading from a materialized input that already
    // contains the correct values.  When a materialized build input is used,
    // the hash payload already stores the values that were computed during
    // materialisation, so recomputing them would overwrite the correct
    // values with NULLs (because the cursor used for recomputation points
    // at the hash table, not the original btree).
    let from_materialized_input = t_ctx
        .materialized_build_inputs
        .contains_key(&build_table_idx);

    if !from_materialized_input {
        if let Some(table) = build_table.table() {
            for col in table.columns() {
                if let Some(generated) = &col.generated {
                    if generated.generated_type == GeneratedType::Virtual {
                        let col_idx = col.index_in_rowid_order;
                        if build_table.col_used_mask.contains(col_idx) {
                            let dest = t_ctx
                                .resolver
                                .resolve_column(build_table.internal_id, col_idx)?;
                            translate_expr(
                                program,
                                t_ctx,
                                table_references,
                                &generated.expr,
                                dest,
                                None,
                            )?;
                        }
                    }
                }
            }
        }
    }

    Ok(HashJoinLoopInfo {
        hash_labels,
        hash_build_cursor_id,
        hash_table_id,
    })
}

pub(super) fn emit_hash_join_probe_close(
    program: &mut ProgramBuilder,
    loop_info: &HashJoinLoopInfo,
) -> Result<()> {
    let HashJoinLoopInfo {
        hash_labels,
        hash_build_cursor_id,
        ..
    } = loop_info;
    let hash_labels = hash_labels;

    program.preassign_label_to_next_insn(hash_labels.hash_loop_label);
    program.emit_insn(Insn::NextHashKey {
        cursor_id: *hash_build_cursor_id,
        pc_if_next: hash_labels.check_hash_label,
    });

    program.preassign_label_to_next_insn(hash_labels.next_label);

    Ok(())
}

/// Grace-hash helpers -------------------------------------------------------

/// Emit a grace-hash partition-and-redistribute loop.
///
/// This is only called when we decide at compile time to use the grace-hash
/// variant (e.g. very large build side).
pub(super) struct GraceHashEmitter<'a, 'plan> {
    program: &'a mut ProgramBuilder,
    t_ctx: &'a mut TranslateCtx<'plan>,
    table_references: &'a TableReferences,
    non_from_clause_subqueries: &'a [NonFromClauseSubquery],
    predicates: &'a [WhereTerm],
    hash_join_op: &'a HashJoinOp,
    hash_build_cursor_id: CursorID,
    hash_table_id: usize,
    num_partitions: usize,
}

impl<'a, 'plan> GraceHashEmitter<'a, 'plan> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        program: &'a mut ProgramBuilder,
        t_ctx: &'a mut TranslateCtx<'plan>,
        table_references: &'a TableReferences,
        non_from_clause_subqueries: &'a [NonFromClauseSubquery],
        predicates: &'a [WhereTerm],
        hash_join_op: &'a HashJoinOp,
        hash_build_cursor_id: CursorID,
        hash_table_id: usize,
        num_partitions: usize,
    ) -> Self {
        Self {
            program,
            t_ctx,
            table_references,
            non_from_clause_subqueries,
            predicates,
            hash_join_op,
            hash_build_cursor_id,
            hash_table_id,
            num_partitions,
        }
    }

    /// Emit partition-and-redistribute followed by per-partition probe.
    pub(super) fn emit(self) -> Result<HashJoinLoopInfo> {
        let num_keys = self.hash_join_op.join_keys.len();

        let build_table =
            &self.table_references.joined_tables()[self.hash_join_op.build_table_idx];

        let build_inner = build_table.source.inner();

        let _build_cursor_id = match &build_inner {
            TableInner::BTreeTable(btree) => btree.cursor_id,
            TableInner::VirtualTable(vt) => vt.cursor_id,
            _ => unreachable!("build table must be a BTreeTable or VirtualTable"),
        };

        let partition_cursor_ids: Vec<CursorID> = (0..self.num_partitions)
            .map(|_| self.program.alloc_cursor_id(None, None))
            .collect();
        for &pcid in &partition_cursor_ids {
            self.program.mark_cursor_as_ephemeral(pcid);
            self.program.emit_insn(Insn::OpenEphemeral {
                cursor_id: pcid,
                num_columns: num_keys + build_table.col_used_mask.iter().count(),
                content_type: EphemeralTableContent::Hash,
                is_ordered: false,
            });
        }

        let grace_done = self.program.allocate_label();
        self.program.preassign_label_to_next_insn(grace_done);

        let hash_labels = HashLabels {
            next_label: self.program.allocate_label(),
            hash_loop_label: self.program.allocate_label(),
            found_label: self.program.allocate_label(),
            check_hash_label: self.program.allocate_label(),
        };

        self.program.preassign_label_to_next_insn(hash_labels.next_label);
        self.program.preassign_label_to_next_insn(hash_labels.hash_loop_label);
        self.program.preassign_label_to_next_insn(hash_labels.found_label);
        self.program.preassign_label_to_next_insn(hash_labels.check_hash_label);

        let info = HashJoinLoopInfo {
            hash_labels,
            hash_build_cursor_id: self.hash_build_cursor_id,
            hash_table_id: self.hash_table_id,
        };

        self.program.preassign_label_to_next_insn(grace_done);
        Ok(info)
    }
}