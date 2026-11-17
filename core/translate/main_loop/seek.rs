use super::*;
use crate::schema::Column;
use crate::translate::expr::expr_type::{column_type, ExprType, TypeEnv};
use crate::translate::expr_context::TableMetadata;
use crate::translate::plan::BitSet;
use crate::vdbe::insn::NullMatchingMask;
use turso_parser::ast::{self, NullsOrder};

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

/// Converts the seek keys in `start_reg..` to the values the index holds.
/// `keys` are the keys' expressions, or None for a key that is not one, such
/// as a value read from a subquery. `in_list_affinity` is the comparison
/// affinity of an IN list's values, which a key gets before it is encoded.
#[allow(clippy::too_many_arguments)]
pub(super) fn encode_seek_keys_for_custom_types<'e>(
    program: &mut ProgramBuilder,
    tables: &TableReferences,
    seek_index: &Arc<Index>,
    start_reg: usize,
    keys: impl IntoIterator<Item = Option<&'e ast::Expr>>,
    idx_col_offset: usize,
    in_list_affinity: Option<&str>,
    resolver: &Resolver<'_>,
) -> crate::Result<()> {
    let table = tables
        .find_table_by_identifier(&seek_index.table_name)
        .or_else(|| tables.find_table_by_table_name(&seek_index.table_name))
        .ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "seek index {} is on table {}, which is not in the query",
                seek_index.name, seek_index.table_name
            ))
        })?;
    for (i, key) in keys.into_iter().enumerate() {
        let idx_col_pos = idx_col_offset + i;
        if idx_col_pos >= seek_index.columns.len() {
            break;
        }
        let idx_col = &seek_index.columns[idx_col_pos];
        let reg = start_reg + i;
        // A scan reads a string literal or parameter compared with a value of
        // a custom type as a value of the type, so the key is converted as the
        // scan converts it. A column holds the stored value; an expression
        // column holds the expression's value, which is the value as read.
        let read_as = match key {
            Some(key) => {
                crate::translate::expr::read_untyped_operand_as(key, resolver.schema(), || {
                    index_column_value_type(table, idx_col, resolver)
                })?
            }
            None => None,
        };
        if let Some(resolved) = read_as {
            crate::translate::expr::emit_untyped_operand_store(program, &resolved, reg, resolver)?;
            if idx_col.expression().is_some() {
                crate::translate::expr::emit_decode(program, &resolved, reg, resolver)?;
            }
            continue;
        }
        if let Some(affinities) = in_list_affinity {
            // ENCODE gets the value the scan compares: `1.0` with INTEGER
            // affinity is `1`, which boolean accepts.
            program.emit_insn(Insn::Affinity {
                start_reg: reg,
                count: std::num::NonZeroUsize::MIN,
                affinities: affinities.to_string(),
            });
        }
        // An expression column holds the expression's value, not a column's.
        if idx_col.expression().is_some() {
            continue;
        }
        let table_col = index_table_column(table, idx_col)?;
        let Some(resolved) = resolver
            .schema()
            .resolve_type(&table_col.ty_str, table.is_strict())?
        else {
            continue;
        };
        // A scan compares the key with the decoded column value. When DECODE
        // returns the stored value unchanged, the key is compared as it is,
        // so the seek must not encode it either.
        if resolved.reads_stored_values_unchanged()
            || resolved.chain.iter().all(|td| td.encode().is_none())
        {
            continue;
        }
        // The index holds encoded values, so the key is encoded too. COERCE
        // does not run: the column's limits apply to what it stores, not to
        // what it is compared with. `n = 3.504` on a numeric(10,2) column
        // matches no row instead of matching 3.50.
        // An array index holds arrays whose elements are encoded. A text key
        // such as '{1,2}' is parsed into an array first, as when it is stored.
        if table_col.is_array() {
            crate::translate::expr::emit_array_leaf_loop(
                program,
                reg,
                table_col.array_dimensions(),
                crate::translate::expr::ArrayInput::TextIfArray,
                |program, leaf_reg| emit_seek_key_encode(program, &resolved, leaf_reg, resolver),
            )?;
        } else {
            emit_seek_key_encode(program, &resolved, reg, resolver)?;
        }
    }
    Ok(())
}

/// The type of the values the index column holds, as a scan reads them.
fn index_column_value_type(
    table: &Table,
    idx_col: &IndexColumn,
    resolver: &Resolver<'_>,
) -> crate::Result<ExprType> {
    match idx_col.expression() {
        Some(expr) => {
            let btree = table.btree().ok_or_else(|| {
                crate::LimboError::InternalError(format!(
                    "expression index on {}, which is not a B-tree table",
                    table.get_name()
                ))
            })?;
            crate::translate::index::expression_type(
                expr,
                &btree,
                resolver.schema(),
                resolver.catalog.symbol_table,
            )
        }
        None => Ok(column_type(
            index_table_column(table, idx_col)?,
            table.is_strict(),
            resolver.schema(),
        )),
    }
}

fn index_table_column<'a>(table: &'a Table, idx_col: &IndexColumn) -> crate::Result<&'a Column> {
    table.columns().get(idx_col.pos_in_table).ok_or_else(|| {
        crate::LimboError::InternalError(format!(
            "index column {} is not a column of table {}",
            idx_col.pos_in_table,
            table.get_name()
        ))
    })
}

/// Emits the ENCODE of each of `chain`'s types on the key in `reg`. NULL is
/// left as it is.
fn emit_seek_key_encode(
    program: &mut ProgramBuilder,
    resolved: &crate::schema::ResolvedType,
    reg: usize,
    resolver: &Resolver<'_>,
) -> crate::Result<()> {
    let skip_label = program.allocate_label();
    program.emit_insn(Insn::IsNull {
        reg,
        target_pc: skip_label,
    });
    crate::translate::expr::emit_encode_for_comparison(
        program,
        &resolved.chain,
        resolved.storage_class()?,
        reg,
        resolver,
    )?;
    program.preassign_label_to_next_insn(skip_label);
    Ok(())
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
                    if !expr.is_nonnull(
                        &TableMetadata::new(Some(self.tables)),
                        &TypeEnv::new(Some(self.tables), &self.t_ctx.resolver),
                    ) && !self.seek_def.is_null_matching_key_component(i)
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
            let keys = self
                .seek_def
                .iter(&self.seek_def.start)
                .map(|key| match key {
                    SeekKeyComponent::Expr(expr) => Some(expr),
                    SeekKeyComponent::Null => None,
                    SeekKeyComponent::None => {
                        unreachable!("None component is not possible in iterator")
                    }
                });
            encode_seek_keys_for_custom_types(
                self.program,
                self.tables,
                idx,
                self.start_reg,
                keys,
                0,
                None,
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
                        [Some(expr)],
                        self.seek_def.prefix.len(),
                        None,
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
                if !expr.is_nonnull(
                    &TableMetadata::new(Some(self.tables)),
                    &TypeEnv::new(Some(self.tables), &self.t_ctx.resolver),
                ) {
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
                collation: CollationSeq::for_internal_values(),
            }),
            (false, SeekOp::GT) => self.program.emit_insn(Insn::Gt {
                lhs: rowid_reg.unwrap(),
                rhs: self.start_reg,
                target_pc: self.loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity.unwrap()),
                collation: CollationSeq::for_internal_values(),
            }),
            (false, SeekOp::LE { .. }) => self.program.emit_insn(Insn::Le {
                lhs: rowid_reg.unwrap(),
                rhs: self.start_reg,
                target_pc: self.loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity.unwrap()),
                collation: CollationSeq::for_internal_values(),
            }),
            (false, SeekOp::LT) => self.program.emit_insn(Insn::Lt {
                lhs: rowid_reg.unwrap(),
                rhs: self.start_reg,
                target_pc: self.loop_end,
                flags: CmpInsFlags::default()
                    .jump_if_null()
                    .with_affinity(affinity.unwrap()),
                collation: CollationSeq::for_internal_values(),
            }),
        }
        Ok(())
    }

    pub(super) fn emit(mut self, loop_start: BranchOffset, use_bloom_filter: bool) -> Result<()> {
        self.emit_start_bound(use_bloom_filter)?;
        self.emit_termination(loop_start)
    }
}
