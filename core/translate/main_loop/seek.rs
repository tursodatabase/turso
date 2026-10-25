use super::*;
use crate::translate::plan::BitSet;
use crate::vdbe::insn::NullMatchingMask;
use turso_parser::ast::NullsOrder;

fn index_seek_affinities(seek_def: &SeekDef, seek_key: &SeekKey) -> String {
    // Apply the constraint's resolved comparison affinity to the seek key,
    // not the indexed column's affinity.
    seek_def
        .iter(seek_key)
        .zip(seek_def.iter_affinity(seek_key))
        .map(|(key_component, aff)| match key_component {
            SeekKeyComponent::Expr(expr) if aff.expr_needs_no_affinity_change(expr) => {
                affinity::SQLITE_AFF_BLOB
            }
            _ => aff.aff_mask(),
        })
        .collect()
}

/// Converts seek keys on custom-type index columns to the form the index
/// stores, so that a seek finds the same rows as a scan with the same filter.
/// `key_exprs[i]` is the expression of key `i`, or None when the key does not
/// come from an expression of its own (e.g. an `IN (subquery)` value).
///
/// When the dialect converts values compared with custom types, a key that
/// has no custom type of its own is converted to the column's type, as the
/// scan does. Any other key is compared by the scan with the decoded column
/// value as it is, so it is used as it is when stored and decoded values look
/// the same, and encoded otherwise (uuid, boolean), which maps a decoded value
/// of the type back to its stored form. Other dialects encode every key.
pub(super) fn encode_seek_keys_for_custom_types(
    program: &mut ProgramBuilder,
    tables: &TableReferences,
    seek_index: &Arc<Index>,
    start_reg: usize,
    key_exprs: &[Option<&Expr>],
    idx_col_offset: usize,
    resolver: &Resolver<'_>,
) -> crate::Result<()> {
    let table = tables
        .find_table_by_identifier(&seek_index.table_name)
        .or_else(|| tables.find_table_by_table_name(&seek_index.table_name));
    let table = match table {
        Some(t) => t,
        None => return Ok(()),
    };
    let columns = table.columns();
    for (i, key_expr) in key_exprs.iter().enumerate() {
        let idx_col_pos = idx_col_offset + i;
        if idx_col_pos >= seek_index.columns.len() {
            break;
        }
        let idx_col = &seek_index.columns[idx_col_pos];
        let table_col = match columns.get(idx_col.pos_in_table) {
            Some(c) => c,
            None => continue,
        };
        let Ok(Some(resolved)) = resolver
            .schema()
            .resolve_type(&table_col.ty_str, table.is_strict())
        else {
            continue;
        };
        let reg = start_reg + i;
        if let Some(key_expr) = key_expr {
            if let Some(converted_reg) = crate::translate::expr::emit_untyped_value_as_type(
                program,
                Some(tables),
                key_expr,
                reg,
                table_col,
                &resolved.chain,
                crate::translate::expr::ValueForm::Stored,
                resolver,
            )? {
                program.emit_insn(Insn::Copy {
                    src_reg: converted_reg,
                    dst_reg: reg,
                    extra_amount: 0,
                });
                continue;
            }
        }
        let converts_compared_values = resolver
            .dialect
            .converts_strings_compared_with_custom_types();
        if (converts_compared_values && resolved.reads_stored_values_unchanged())
            || resolved
                .chain
                .iter()
                .all(|td| seek_key_encode(td, converts_compared_values).is_none())
        {
            continue;
        }
        let skip_label = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg,
            target_pc: skip_label,
        });
        for td in &resolved.chain {
            let Some(encode_expr) = seek_key_encode(td, converts_compared_values) else {
                continue;
            };
            if td.user_params().next().is_some()
                && resolver
                    .dialect
                    .compares_values_without_column_type_parameters()
            {
                encode_seek_key_that_parameters_would_change(
                    program,
                    encode_expr,
                    reg,
                    table_col,
                    td,
                    resolver,
                )?;
            } else {
                crate::translate::expr::emit_type_expr(
                    program,
                    encode_expr,
                    reg,
                    reg,
                    table_col,
                    td,
                    resolver,
                )?;
            }
        }
        program.preassign_label_to_next_insn(skip_label);
    }
    Ok(())
}

/// Encodes a seek key with the column's type parameters, so that `1.5` sought
/// in a `numeric(10,2)` index becomes the stored form of `1.50`. When the
/// parameters change the value, as they round `1.501` to `1.50`, no stored
/// value equals the key, so the key is encoded without the parameters
/// instead: that form is never stored in the index, so the seek finds no row.
fn encode_seek_key_that_parameters_would_change(
    program: &mut ProgramBuilder,
    encode_expr: &Expr,
    reg: usize,
    table_col: &crate::schema::Column,
    td: &crate::schema::TypeDef,
    resolver: &Resolver<'_>,
) -> crate::Result<()> {
    let eq_func_name = td
        .operators()
        .iter()
        .find(|o| o.op == "=")
        .and_then(|o| o.func_name.as_deref())
        .ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "type {} has parameters but no = function",
                td.name
            ))
        })?;
    let eq_func = resolver.resolve_function(eq_func_name, 2)?.ok_or_else(|| {
        crate::LimboError::InternalError(format!("function not found: {eq_func_name}"))
    })?;
    let keys = program.alloc_registers(2);
    let (with_parameters, without_parameters) = (keys, keys + 1);
    crate::translate::expr::emit_type_expr(
        program,
        encode_expr,
        reg,
        with_parameters,
        table_col,
        td,
        resolver,
    )?;
    crate::translate::expr::emit_type_expr_without_type_parameters(
        program,
        encode_expr,
        reg,
        without_parameters,
        table_col,
        td,
        resolver,
    )?;
    let unchanged = program.alloc_register();
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: keys,
        dest: unchanged,
        func: crate::function::FuncCtx {
            func: eq_func,
            arg_count: 2,
        },
    });
    let changed_label = program.allocate_label();
    let done_label = program.allocate_label();
    program.emit_insn(Insn::IfNot {
        reg: unchanged,
        target_pc: changed_label,
        jump_if_null: true,
    });
    program.emit_insn(Insn::Copy {
        src_reg: with_parameters,
        dst_reg: reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Goto {
        target_pc: done_label,
    });
    program.preassign_label_to_next_insn(changed_label);
    program.emit_insn(Insn::Copy {
        src_reg: without_parameters,
        dst_reg: reg,
        extra_amount: 0,
    });
    program.preassign_label_to_next_insn(done_label);
    Ok(())
}

fn seek_key_encode(td: &crate::schema::TypeDef, converts_compared_values: bool) -> Option<&Expr> {
    if converts_compared_values {
        td.encode_for_comparison_value()
    } else {
        td.encode()
    }
}

/// Seek-based loop setup.
///
/// A seek loop has a real two-phase contract:
/// 1. Emit and position using the start bound.
/// 2. Emit the termination bound and anchor `loop_start`.
pub(super) struct SeekEmitter<'a, 'plan> {
    program: &'a mut ProgramBuilder,
    tables: &'a TableReferences,
    seek_def: &'a SeekDef,
    t_ctx: &'a mut TranslateCtx<'plan>,
    seek_cursor_id: usize,
    start_reg: usize,
    loop_end: BranchOffset,
    seek_index: Option<&'a Arc<Index>>,
    is_index: bool,
}

impl<'a, 'plan> SeekEmitter<'a, 'plan> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        program: &'a mut ProgramBuilder,
        tables: &'a TableReferences,
        seek_def: &'a SeekDef,
        t_ctx: &'a mut TranslateCtx<'plan>,
        seek_cursor_id: usize,
        start_reg: usize,
        loop_end: BranchOffset,
        seek_index: Option<&'a Arc<Index>>,
    ) -> Self {
        Self {
            program,
            tables,
            seek_def,
            t_ctx,
            seek_cursor_id,
            start_reg,
            loop_end,
            seek_index,
            is_index: seek_index.is_some(),
        }
    }

    /// Emit the start bound and position the cursor at the first candidate row.
    fn emit_start_bound(&mut self, use_bloom_filter: bool) -> Result<()> {
        if self.seek_def.prefix.is_empty()
            && matches!(self.seek_def.start.last_component, SeekKeyComponent::None)
        {
            match self.seek_def.iter_dir {
                IterationDirection::Forwards => {
                    if self.seek_index.is_some_and(|index| {
                        index.columns[0].effective_nulls_order() == NullsOrder::First
                    }) {
                        self.program.emit_null(self.start_reg, None);
                        self.program.emit_insn(Insn::SeekGT {
                            is_index: self.is_index,
                            cursor_id: self.seek_cursor_id,
                            start_reg: self.start_reg,
                            num_regs: 1,
                            target_pc: self.loop_end,
                        });
                    } else {
                        self.program.emit_insn(Insn::Rewind {
                            cursor_id: self.seek_cursor_id,
                            pc_if_empty: self.loop_end,
                        });
                    }
                }
                IterationDirection::Backwards => {
                    if self.seek_index.is_some_and(|index| {
                        index.columns[0].effective_nulls_order() == NullsOrder::Last
                    }) {
                        self.program.emit_null(self.start_reg, None);
                        self.program.emit_insn(Insn::SeekLT {
                            is_index: self.is_index,
                            cursor_id: self.seek_cursor_id,
                            start_reg: self.start_reg,
                            num_regs: 1,
                            target_pc: self.loop_end,
                        });
                    } else {
                        self.program.emit_insn(Insn::Last {
                            cursor_id: self.seek_cursor_id,
                            pc_if_empty: self.loop_end,
                        });
                    }
                }
            }
            return Ok(());
        }

        for (i, key) in self.seek_def.iter(&self.seek_def.start).enumerate() {
            let reg = self.start_reg + i;
            match key {
                SeekKeyComponent::Expr(expr) => {
                    translate_expr_no_constant_opt(
                        self.program,
                        Some(self.tables),
                        expr,
                        reg,
                        &self.t_ctx.resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                    // A NULL key can never satisfy `=`, so the loop is done as
                    // soon as one shows up. `IS` matches NULL instead: keep the
                    // NULL in the seek register and let the index comparison
                    // find the rows whose key component is NULL.
                    if !expr.is_nonnull(self.tables)
                        && !self.seek_def.is_null_matching_key_component(i)
                    {
                        self.program.emit_insn(Insn::IsNull {
                            reg,
                            target_pc: self.loop_end,
                        });
                    }
                }
                SeekKeyComponent::Null => self.program.emit_null(reg, None),
                SeekKeyComponent::None => {
                    unreachable!("None component is not possible in iterator")
                }
            }
        }
        let num_regs = self.seek_def.size(&self.seek_def.start);
        // Which key components match NULL rather than comparing with `=`; the
        // seek and the bloom-filter probe keep their "NULL key cannot match"
        // shortcut for the rest.
        let mut null_matching_bits = BitSet::default();
        for i in 0..num_regs {
            if self.seek_def.is_null_matching_key_component(i) {
                null_matching_bits.set(i)?;
            }
        }
        let null_matching_mask = NullMatchingMask::from(null_matching_bits);

        if let Some(idx) = self.seek_index {
            let key_exprs: Vec<Option<&Expr>> = self
                .seek_def
                .iter(&self.seek_def.start)
                .map(|key| match key {
                    SeekKeyComponent::Expr(expr) => Some(expr),
                    SeekKeyComponent::Null | SeekKeyComponent::None => None,
                })
                .collect();
            encode_seek_keys_for_custom_types(
                self.program,
                self.tables,
                idx,
                self.start_reg,
                &key_exprs,
                0,
                &self.t_ctx.resolver,
            )?;
            let affinities = index_seek_affinities(self.seek_def, &self.seek_def.start);
            if affinities.chars().any(|c| c != affinity::SQLITE_AFF_BLOB) {
                self.program.emit_insn(Insn::Affinity {
                    start_reg: self.start_reg,
                    count: std::num::NonZeroUsize::new(num_regs).unwrap(),
                    affinities,
                });
            }
            if use_bloom_filter {
                turso_assert!(
                    idx.ephemeral,
                    "bloom filter can only be used with ephemeral indexes"
                );
                // The probe treats a NULL key as "definitely absent", which
                // would skip rows whose key IS NULL. `emit_autoindex` never
                // builds a filter for a NULL-matching seek, so probing one
                // here means the build and probe decisions have diverged.
                turso_assert!(
                    null_matching_mask.is_empty(),
                    "a NULL-matching seek must not probe a bloom filter"
                );
                self.program.emit_insn(Insn::Filter {
                    cursor_id: self.seek_cursor_id,
                    key_reg: self.start_reg,
                    num_keys: num_regs,
                    target_pc: self.loop_end,
                });
            }
        }

        match self.seek_def.start.op {
            SeekOp::GE { eq_only } => self.program.emit_insn(Insn::SeekGE {
                is_index: self.is_index,
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
                eq_only,
                null_matching_mask,
            }),
            SeekOp::GT => self.program.emit_insn(Insn::SeekGT {
                is_index: self.is_index,
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
            }),
            SeekOp::LE { eq_only } => self.program.emit_insn(Insn::SeekLE {
                is_index: self.is_index,
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
                eq_only,
                null_matching_mask,
            }),
            SeekOp::LT => self.program.emit_insn(Insn::SeekLT {
                is_index: self.is_index,
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
            }),
        };

        Ok(())
    }

    /// Emit the end bound check and anchor the loop-start label.
    fn emit_termination(&mut self, loop_start: BranchOffset) -> Result<()> {
        if self.seek_def.prefix.is_empty()
            && matches!(self.seek_def.end.last_component, SeekKeyComponent::None)
        {
            self.program.preassign_label_to_next_insn(loop_start);
            match self.seek_def.iter_dir {
                IterationDirection::Forwards => {
                    if self.seek_index.is_some_and(|index| {
                        index.columns[0].effective_nulls_order() == NullsOrder::Last
                    }) {
                        self.program.emit_null(self.start_reg, None);
                        self.program.emit_insn(Insn::IdxGE {
                            cursor_id: self.seek_cursor_id,
                            start_reg: self.start_reg,
                            num_regs: 1,
                            target_pc: self.loop_end,
                        });
                    }
                }
                IterationDirection::Backwards => {
                    if self.seek_index.is_some_and(|index| {
                        index.columns[0].effective_nulls_order() == NullsOrder::First
                    }) {
                        self.program.emit_null(self.start_reg, None);
                        self.program.emit_insn(Insn::IdxLE {
                            cursor_id: self.seek_cursor_id,
                            start_reg: self.start_reg,
                            num_regs: 1,
                            target_pc: self.loop_end,
                        });
                    }
                }
            }
            return Ok(());
        }

        let num_regs = self.seek_def.size(&self.seek_def.end);
        let last_reg = self.start_reg + self.seek_def.prefix.len();
        match &self.seek_def.end.last_component {
            SeekKeyComponent::Expr(expr) => {
                translate_expr_no_constant_opt(
                    self.program,
                    Some(self.tables),
                    expr,
                    last_reg,
                    &self.t_ctx.resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                if let Some(idx) = self.seek_index {
                    encode_seek_keys_for_custom_types(
                        self.program,
                        self.tables,
                        idx,
                        last_reg,
                        &[Some(expr)],
                        self.seek_def.prefix.len(),
                        &self.t_ctx.resolver,
                    )?;
                    let affinities = index_seek_affinities(self.seek_def, &self.seek_def.end);
                    if affinities.chars().any(|c| c != affinity::SQLITE_AFF_BLOB) {
                        self.program.emit_insn(Insn::Affinity {
                            start_reg: self.start_reg,
                            count: std::num::NonZeroUsize::new(num_regs).unwrap(),
                            affinities,
                        });
                    }
                }
                if !expr.is_nonnull(self.tables) {
                    self.program.emit_insn(Insn::IsNull {
                        reg: last_reg,
                        target_pc: self.loop_end,
                    });
                }
            }
            SeekKeyComponent::Null => self.program.emit_null(last_reg, None),
            SeekKeyComponent::None => {}
        }

        self.program.preassign_label_to_next_insn(loop_start);
        let mut rowid_reg = None;
        let mut affinity = None;
        if !self.is_index {
            rowid_reg = Some(self.program.alloc_register());
            self.program.emit_insn(Insn::RowId {
                cursor_id: self.seek_cursor_id,
                dest: rowid_reg.unwrap(),
            });

            affinity = if let Some(table_ref) = self
                .tables
                .joined_tables()
                .iter()
                .find(|t| t.columns().iter().any(|c| c.is_rowid_alias()))
            {
                if let Some(rowid_col_idx) =
                    table_ref.columns().iter().position(|c| c.is_rowid_alias())
                {
                    Some(table_ref.columns()[rowid_col_idx].affinity())
                } else {
                    Some(Affinity::Numeric)
                }
            } else {
                Some(Affinity::Numeric)
            };
        }

        match (self.is_index, self.seek_def.end.op) {
            (true, SeekOp::GE { .. }) => self.program.emit_insn(Insn::IdxGE {
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
            }),
            (true, SeekOp::GT) => self.program.emit_insn(Insn::IdxGT {
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
            }),
            (true, SeekOp::LE { .. }) => self.program.emit_insn(Insn::IdxLE {
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
            }),
            (true, SeekOp::LT) => self.program.emit_insn(Insn::IdxLT {
                cursor_id: self.seek_cursor_id,
                start_reg: self.start_reg,
                num_regs,
                target_pc: self.loop_end,
            }),
            (false, SeekOp::GE { .. }) => self.program.emit_insn(Insn::Ge {
                lhs: rowid_reg.unwrap(),
                rhs: self.start_reg,
                target_pc: self.loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity.unwrap()),
                collation: self.program.curr_collation(),
            }),
            (false, SeekOp::GT) => self.program.emit_insn(Insn::Gt {
                lhs: rowid_reg.unwrap(),
                rhs: self.start_reg,
                target_pc: self.loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity.unwrap()),
                collation: self.program.curr_collation(),
            }),
            (false, SeekOp::LE { .. }) => self.program.emit_insn(Insn::Le {
                lhs: rowid_reg.unwrap(),
                rhs: self.start_reg,
                target_pc: self.loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity.unwrap()),
                collation: self.program.curr_collation(),
            }),
            (false, SeekOp::LT) => self.program.emit_insn(Insn::Lt {
                lhs: rowid_reg.unwrap(),
                rhs: self.start_reg,
                target_pc: self.loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity.unwrap()),
                collation: self.program.curr_collation(),
            }),
        }
        Ok(())
    }

    pub(super) fn emit(mut self, loop_start: BranchOffset, use_bloom_filter: bool) -> Result<()> {
        self.emit_start_bound(use_bloom_filter)?;
        self.emit_termination(loop_start)
    }
}
