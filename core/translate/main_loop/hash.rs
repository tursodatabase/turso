use super::*;

#[derive(Debug, Clone)]
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

struct HashBuildPlanner<'program, 'ctx, 'plan, 'tables, 'predicates> {
    program: &'program mut ProgramBuilder,
    t_ctx: &'ctx mut TranslateCtx<'plan>,
    table_references: &'tables TableReferences,
    non_from_clause_subqueries: &'predicates [NonFromClauseSubquery],
    predicates: &'predicates [WhereTerm],
    hash_join_op: &'predicates HashJoinOp,
    hash_build_cursor_id: CursorID,
    hash_table_id: usize,
}

struct PreparedHashBuild<'program, 'ctx, 'plan, 'tables, 'predicates> {
    planner: HashBuildPlanner<'program, 'ctx, 'plan, 'tables, 'predicates>,
    config: HashBuildConfig,
}

enum HashBuildPlan<'program, 'ctx, 'plan, 'tables, 'predicates> {
    Reuse(HashBuildPayloadInfo),
    Build(PreparedHashBuild<'program, 'ctx, 'plan, 'tables, 'predicates>),
}

impl<'program, 'ctx, 'plan, 'tables, 'predicates>
    HashBuildPlanner<'program, 'ctx, 'plan, 'tables, 'predicates>
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        program: &'program mut ProgramBuilder,
        t_ctx: &'ctx mut TranslateCtx<'plan>,
        table_references: &'tables TableReferences,
        non_from_clause_subqueries: &'predicates [NonFromClauseSubquery],
        predicates: &'predicates [WhereTerm],
        hash_join_op: &'predicates HashJoinOp,
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

    fn prepare(self) -> Result<HashBuildPlan<'program, 'ctx, 'plan, 'tables, 'predicates>> {
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
        let signature = HashBuildSignature {
            join_key_indices,
            payload_refs: payload_columns.clone(),
            key_affinities: key_affinities.clone(),
            use_bloom_filter,
            materialized_input_cursor: materialized_cursor_id,
            materialized_mode: materialized_input.as_ref().map(|input| match input.mode {
                MaterializedBuildInputMode::RowidOnly => MaterializedBuildInputModeTag::RowidOnly,
                MaterializedBuildInputMode::KeyPayload { .. } => {
                    MaterializedBuildInputModeTag::Payload
                }
            }),
        };

        if self
            .program
            .hash_build_signature_matches(self.hash_table_id, &signature)
        {
            return Ok(HashBuildPlan::Reuse(HashBuildPayloadInfo {
                payload_columns,
                key_affinities,
                use_bloom_filter,
                bloom_filter_cursor_id,
                allow_seek,
            }));
        }
        if self.program.has_hash_build_signature(self.hash_table_id) {
            self.program.emit_insn(Insn::HashClose {
                hash_table_id: self.hash_table_id,
            });
            self.program.clear_hash_build_signature(self.hash_table_id);
        }

        Ok(HashBuildPlan::Build(PreparedHashBuild {
            planner: self,
            config: HashBuildConfig {
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
            },
        }))
    }
}

impl<'program, 'ctx, 'plan, 'tables, 'predicates>
    PreparedHashBuild<'program, 'ctx, 'plan, 'tables, 'predicates>
{
    fn emit(self) -> Result<HashBuildPayloadInfo> {
        let Self { planner, config } = self;
        let build_table =
            &planner.table_references.joined_tables()[planner.hash_join_op.build_table_idx];
        let btree = build_table
            .btree()
            .expect("Hash join build table must be a BTree table");
        let num_keys = planner.hash_join_op.join_keys.len();

        let build_key_start_reg = planner.program.alloc_registers(num_keys);
        let mut build_rowid_reg = None;
        let mut build_iter_cursor_id = planner.hash_build_cursor_id;
        let materialized_input = planner
            .t_ctx
            .materialized_build_inputs
            .get(&planner.hash_join_op.build_table_idx);
        if let Some(input) = materialized_input {
            match &input.mode {
                MaterializedBuildInputMode::RowidOnly => {
                    build_rowid_reg = Some(planner.program.alloc_register());
                    build_iter_cursor_id = input.cursor_id;
                }
                MaterializedBuildInputMode::KeyPayload { .. } => {
                    build_iter_cursor_id = input.cursor_id;
                }
            }
        }

        let (key_source_cursor_id, payload_source_cursor_id, hash_build_rowid_cursor_id) =
            if config.use_materialized_keys {
                (
                    build_iter_cursor_id,
                    build_iter_cursor_id,
                    build_iter_cursor_id,
                )
            } else {
                (
                    planner.hash_build_cursor_id,
                    planner.hash_build_cursor_id,
                    planner.hash_build_cursor_id,
                )
            };

        let build_loop_start = planner.program.allocate_label();
        let build_loop_end = planner.program.allocate_label();
        let skip_to_next = planner.program.allocate_label();
        let label_hash_build_end = planner.program.allocate_label();
        planner.program.emit_insn(Insn::Once {
            target_pc_when_reentered: label_hash_build_end,
        });

        if !config.use_materialized_keys {
            planner.program.emit_insn(Insn::OpenRead {
                cursor_id: planner.hash_build_cursor_id,
                root_page: btree.root_page,
                db: build_table.database_id,
            });
        }

        planner.program.emit_insn(Insn::Rewind {
            cursor_id: build_iter_cursor_id,
            pc_if_empty: build_loop_end,
        });

        if !config.use_materialized_keys {
            planner
                .program
                .set_cursor_override(build_table.internal_id, planner.hash_build_cursor_id);
        }

        planner
            .program
            .preassign_label_to_next_insn(build_loop_start);

        if let (Some(rowid_reg), Some(input_cursor_id)) =
            (build_rowid_reg, config.materialized_cursor_id)
        {
            planner
                .program
                .emit_column_or_rowid(input_cursor_id, 0, rowid_reg);
            planner.program.emit_insn(Insn::SeekRowid {
                cursor_id: planner.hash_build_cursor_id,
                src_reg: rowid_reg,
                target_pc: skip_to_next,
            });
        }

        if !config.use_materialized_keys {
            let build_only_mask = TableMask::from_table_number_iter(
                [planner.hash_join_op.build_table_idx].into_iter(),
            );
            for cond in planner.predicates.iter() {
                let mask = table_mask_from_expr(
                    &cond.expr,
                    planner.table_references,
                    planner.non_from_clause_subqueries,
                )?;
                if !mask.contains_table(planner.hash_join_op.build_table_idx)
                    || !build_only_mask.contains_all(&mask)
                {
                    continue;
                }
                if expr_references_outer_query(&cond.expr, planner.table_references) {
                    continue;
                }
                let jump_target_when_true = planner.program.allocate_label();
                let condition_metadata = ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_true,
                    jump_target_when_false: skip_to_next,
                    jump_target_when_null: skip_to_next,
                };
                translate_condition_expr(
                    planner.program,
                    planner.table_references,
                    &cond.expr,
                    condition_metadata,
                    &planner.t_ctx.resolver,
                )?;
                planner
                    .program
                    .preassign_label_to_next_insn(jump_target_when_true);
            }
        }

        if config.use_materialized_keys {
            for idx in 0..num_keys {
                planner.program.emit_column_or_rowid(
                    key_source_cursor_id,
                    idx,
                    build_key_start_reg + idx,
                );
            }
        } else {
            for (idx, join_key) in planner.hash_join_op.join_keys.iter().enumerate() {
                let build_expr = join_key.get_build_expr(planner.predicates);
                translate_expr(
                    planner.program,
                    Some(planner.table_references),
                    build_expr,
                    build_key_start_reg + idx,
                    &planner.t_ctx.resolver,
                )?;
            }
        }

        if let Some(count) = std::num::NonZeroUsize::new(num_keys) {
            planner.program.emit_insn(Insn::Affinity {
                start_reg: build_key_start_reg,
                count,
                affinities: config.key_affinities.clone(),
            });
        }

        let num_payload = config.payload_columns.len();
        let (payload_start_reg, mut payload_info) = if num_payload > 0 {
            let payload_reg = planner.program.alloc_registers(num_payload);
            for (i, &col_idx) in config.payload_signature_columns.iter().enumerate() {
                planner.program.emit_column_or_rowid(
                    payload_source_cursor_id,
                    col_idx,
                    payload_reg + i,
                );
            }
            (
                Some(payload_reg),
                HashBuildPayloadInfo {
                    payload_columns: config.payload_columns.clone(),
                    key_affinities: config.key_affinities.clone(),
                    use_bloom_filter: false,
                    bloom_filter_cursor_id: config.bloom_filter_cursor_id,
                    allow_seek: config.allow_seek,
                },
            )
        } else {
            (
                None,
                HashBuildPayloadInfo {
                    payload_columns: vec![],
                    key_affinities: config.key_affinities.clone(),
                    use_bloom_filter: false,
                    bloom_filter_cursor_id: config.bloom_filter_cursor_id,
                    allow_seek: config.allow_seek,
                },
            )
        };

        if !config.use_materialized_keys {
            planner
                .program
                .clear_cursor_override(build_table.internal_id);
        }

        planner.program.emit_insn(Insn::HashBuild {
            data: Box::new(HashBuildData {
                cursor_id: hash_build_rowid_cursor_id,
                key_start_reg: build_key_start_reg,
                num_keys,
                hash_table_id: planner.hash_table_id,
                mem_budget: planner.hash_join_op.mem_budget,
                collations: config.collations,
                payload_start_reg,
                num_payload,
                track_matched: matches!(
                    planner.hash_join_op.join_type,
                    HashJoinType::LeftOuter | HashJoinType::FullOuter
                ),
            }),
        });
        if config.use_bloom_filter {
            planner.program.emit_insn(Insn::FilterAdd {
                cursor_id: config.bloom_filter_cursor_id,
                key_reg: build_key_start_reg,
                num_keys,
            });
            payload_info.use_bloom_filter = true;
        }

        planner.program.preassign_label_to_next_insn(skip_to_next);
        planner.program.emit_insn(Insn::Next {
            cursor_id: build_iter_cursor_id,
            pc_if_next: build_loop_start,
        });

        planner.program.preassign_label_to_next_insn(build_loop_end);
        planner.program.emit_insn(Insn::HashBuildFinalize {
            hash_table_id: planner.hash_table_id,
        });
        planner
            .program
            .record_hash_build_signature(planner.hash_table_id, config.signature);

        planner
            .program
            .preassign_label_to_next_insn(label_hash_build_end);
        Ok(payload_info)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_hash_build_phase(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'_>,
    table_references: &TableReferences,
    non_from_clause_subqueries: &[NonFromClauseSubquery],
    predicates: &[WhereTerm],
    hash_join_op: &HashJoinOp,
    hash_build_cursor_id: CursorID,
    hash_table_id: usize,
) -> Result<HashBuildPayloadInfo> {
    match HashBuildPlanner::new(
        program,
        t_ctx,
        table_references,
        non_from_clause_subqueries,
        predicates,
        hash_join_op,
        hash_build_cursor_id,
        hash_table_id,
    )
    .prepare()?
    {
        HashBuildPlan::Reuse(info) => Ok(info),
        HashBuildPlan::Build(prepared) => prepared.emit(),
    }
}
