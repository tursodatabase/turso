use super::*;
use crate::function::{Func, FuncCtx, ScalarFunc};
use crate::functions::seek_key::{exact_bound_key, NoSeekKey};
use crate::schema::{IndexUse, SeekKeyFunction};
use crate::translate::plan::BitSet;
use crate::vdbe::insn::NullMatchingMask;
use turso_parser::ast::NullsOrder;

fn index_seek_affinities(seek_def: &SeekDef, seek_key: &SeekKey) -> String {
    // Apply the constraint's resolved comparison affinity to the seek key,
    // not the indexed column's affinity.
    seek_def
        .iter(seek_key)
        .zip(seek_def.iter_affinity(seek_key))
        .enumerate()
        .map(|(pos, (key_component, aff))| match key_component {
            _ if seek_def.key_component_index_use(pos) != IndexUse::Plain => {
                affinity::SQLITE_AFF_BLOB
            }
            SeekKeyComponent::Expr(expr) if aff.expr_needs_no_affinity_change(expr) => {
                affinity::SQLITE_AFF_BLOB
            }
            _ => aff.aff_mask(),
        })
        .collect()
}

#[derive(Clone, Copy)]
enum StoredRangeEnd {
    Low,
    High,
}

/// Seek-based loop setup.
///
/// A seek loop has a real two-phase contract:
/// 1. Emit and position using the start bound.
/// 2. Emit the termination bound and anchor `loop_start`.
pub(super) struct SeekEmitter<'a, 'plan> {
    program: &'a mut ProgramBuilder,
    tables: &'a TableReferences,
    table: &'a JoinedTable,
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
        table: &'a JoinedTable,
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
            table,
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

        let seek_def = self.seek_def;
        for (i, key) in seek_def.iter(&seek_def.start).enumerate() {
            let reg = self.start_reg + i;
            match key {
                SeekKeyComponent::Expr(expr) => {
                    let operand_reg = self.operand_register(i, reg);
                    translate_expr_no_constant_opt(
                        self.program,
                        Some(self.tables),
                        expr,
                        operand_reg,
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
                            reg: operand_reg,
                            target_pc: self.loop_end,
                        });
                    }
                    let start_key_range_end = match self.seek_def.iter_dir {
                        IterationDirection::Forwards => StoredRangeEnd::Low,
                        IterationDirection::Backwards => StoredRangeEnd::High,
                    };
                    self.emit_index_key(
                        i,
                        operand_reg,
                        reg,
                        expr,
                        &seek_def.start,
                        start_key_range_end,
                    )?;
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

        let seek_def = self.seek_def;
        let num_regs = seek_def.size(&seek_def.end);
        let last_reg = self.start_reg + seek_def.prefix.len();
        match &seek_def.end.last_component {
            SeekKeyComponent::Expr(expr) => {
                let operand_reg = self.operand_register(seek_def.prefix.len(), last_reg);
                translate_expr_no_constant_opt(
                    self.program,
                    Some(self.tables),
                    expr,
                    operand_reg,
                    &self.t_ctx.resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                if !expr.is_nonnull(self.tables) {
                    self.program.emit_insn(Insn::IsNull {
                        reg: operand_reg,
                        target_pc: self.loop_end,
                    });
                }
                let end_key_range_end = match self.seek_def.iter_dir {
                    IterationDirection::Forwards => StoredRangeEnd::High,
                    IterationDirection::Backwards => StoredRangeEnd::Low,
                };
                self.emit_index_key(
                    seek_def.prefix.len(),
                    operand_reg,
                    last_reg,
                    expr,
                    &seek_def.end,
                    end_key_range_end,
                )?;
                if self.seek_index.is_some() {
                    let affinities = index_seek_affinities(self.seek_def, &self.seek_def.end);
                    if affinities.chars().any(|c| c != affinity::SQLITE_AFF_BLOB) {
                        self.program.emit_insn(Insn::Affinity {
                            start_reg: self.start_reg,
                            count: std::num::NonZeroUsize::new(num_regs).unwrap(),
                            affinities,
                        });
                    }
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

    fn operand_register(&mut self, pos: usize, key_reg: usize) -> usize {
        match self.seek_def.key_component_index_use(pos) {
            IndexUse::KeyFunction(SeekKeyFunction::PgNumeric) => self.program.alloc_registers(3),
            IndexUse::KeyFunction(_) => self.program.alloc_registers(2),
            _ => key_reg,
        }
    }

    fn emit_index_key(
        &mut self,
        pos: usize,
        operand_reg: usize,
        key_reg: usize,
        operand: &Expr,
        seek_key: &SeekKey,
        range_end: StoredRangeEnd,
    ) -> Result<()> {
        let index_use = self.seek_def.key_component_index_use(pos);
        if index_use == IndexUse::Plain {
            return Ok(());
        }
        let index = self
            .seek_index
            .expect("only an index seek turns an operand into an index key");
        let is_equality = pos < self.seek_def.prefix.len();
        match index_use {
            IndexUse::KeyFunction(function) => {
                let affinity = self
                    .seek_def
                    .iter_affinity(seek_key)
                    .nth(pos)
                    .expect("key component must have an affinity");
                if function != SeekKeyFunction::PgNumeric
                    && !affinity.expr_needs_no_affinity_change(operand)
                {
                    self.program.emit_insn(Insn::Affinity {
                        start_reg: operand_reg,
                        count: std::num::NonZeroUsize::MIN,
                        affinities: affinity.aff_mask().to_string(),
                    });
                }
                let no_key = if is_equality {
                    NoSeekKey::Null
                } else if function.gives_exact_bounds() {
                    exact_bound_key(seek_key.op, index.columns[pos].order)
                } else {
                    match (range_end, index.columns[pos].order) {
                        (StoredRangeEnd::Low, SortOrder::Asc)
                        | (StoredRangeEnd::High, SortOrder::Desc) => NoSeekKey::Below,
                        (StoredRangeEnd::High, SortOrder::Asc)
                        | (StoredRangeEnd::Low, SortOrder::Desc) => NoSeekKey::Above,
                    }
                };
                self.program.emit_int(no_key as i64, operand_reg + 1);
                let mut arg_count = 2;
                if function == SeekKeyFunction::PgNumeric {
                    let column = &self.table.columns()[index.columns[pos].pos_in_table];
                    let scale = column
                        .ty_params
                        .get(1)
                        .expect("a pg_numeric column has a precision and a scale");
                    translate_expr(
                        self.program,
                        None,
                        scale,
                        operand_reg + 2,
                        &self.t_ctx.resolver,
                    )?;
                    arg_count = 3;
                }
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: operand_reg,
                    dest: key_reg,
                    func: FuncCtx {
                        func: Func::Scalar(function.scalar_func()),
                        arg_count,
                    },
                });
            }
            IndexUse::NumericEquality => {
                turso_assert!(is_equality, "a numeric key can only seek for equality");
                let column = &self.table.columns()[index.columns[pos].pos_in_table];
                let [precision, scale] = column.ty_params.as_slice() else {
                    unreachable!("a numeric column has a precision and a scale");
                };
                let args = self.program.alloc_registers(3);
                self.program.emit_insn(Insn::Copy {
                    src_reg: operand_reg,
                    dst_reg: args,
                    extra_amount: 0,
                });
                translate_expr(
                    self.program,
                    None,
                    precision,
                    args + 1,
                    &self.t_ctx.resolver,
                )?;
                translate_expr(self.program, None, scale, args + 2, &self.t_ctx.resolver)?;
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: args,
                    dest: key_reg,
                    func: FuncCtx {
                        func: Func::Scalar(ScalarFunc::NumericSeekKey),
                        arg_count: 3,
                    },
                });
            }
            IndexUse::Plain | IndexUse::Unusable => {
                unreachable!("{index_use:?} never turns an operand into an index key")
            }
        }
        if is_equality {
            self.program.emit_insn(Insn::IsNull {
                reg: key_reg,
                target_pc: self.loop_end,
            });
        }
        Ok(())
    }
}
