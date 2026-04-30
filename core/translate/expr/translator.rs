use super::*;

/// Reason why [translate_expr_no_constant_opt()] was called.
#[derive(Debug)]
pub enum NoConstantOptReason {
    /// The expression translation involves reusing register(s),
    /// so hoisting those register assignments is not safe.
    /// e.g. SELECT COALESCE(1, t.x, NULL) would overwrite 1 with NULL, which is invalid.
    RegisterReuse,
    /// The column has a custom type encode function that will be applied
    /// in-place after this expression is evaluated. We must not hoist the
    /// expression because:
    ///
    /// 1. The encode function may be non-deterministic (e.g. it could use
    ///    datetime('now')), so hoisting would produce incorrect results.
    ///
    /// 2. Even if the encode function were deterministic, the encode is
    ///    applied in-place to the target register inside the update loop.
    ///    If the original value were hoisted (evaluated once before the
    ///    loop), the second iteration would read the already-encoded value
    ///    from the register and encode it again, causing progressive
    ///    double-encoding (e.g. 99 → 9900 → 990000 → ...).
    ///
    /// The correct fix for deterministic encode functions would be to hoist
    /// the *encoded* result (i.e. `encode_fn(99)` not `99`), but that
    /// requires tracking the encode through the hoisting machinery. For now
    /// we simply disable hoisting for these columns.
    CustomTypeEncode,
    /// IN-list values are inserted into an ephemeral table in a loop.
    /// Each value reuses the same register, so hoisting would collapse
    /// all values into the last one.
    InListEphemeral,
}

/// Controls how binary expressions are emitted.
///
/// This makes scalar and row-valued paths explicit:
/// - scalar binary expressions use mode to pick either value emission or conditional jump emission
/// - row-valued binary expressions always emit a value register first, then optionally a conditional jump
#[derive(Clone, Copy)]
pub(super) enum BinaryEmitMode {
    Value,
    Condition(ConditionMetadata),
}

/// Translate an expression into bytecode via [translate_expr()], and forbid any constant values from being hoisted
/// into the beginning of the program. This is a good idea in most cases where
/// a register will end up being reused e.g. in a coroutine.
pub fn translate_expr_no_constant_opt(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_register: usize,
    resolver: &Resolver,
    deopt_reason: NoConstantOptReason,
) -> Result<usize> {
    tracing::debug!(
        "translate_expr_no_constant_opt: expr={:?}, deopt_reason={:?}",
        expr,
        deopt_reason
    );
    let next_span_idx = program.constant_spans_next_idx();
    let translated = translate_expr(program, referenced_tables, expr, target_register, resolver)?;
    program.constant_spans_invalidate_after(next_span_idx);
    Ok(translated)
}

/// Resolve an expression to a register, reusing an existing register when possible.
///
/// Unlike `translate_expr`, this does not require a pre-allocated target register.
/// If the expression is found in the `expr_to_reg_cache`, the cached register is
/// returned directly without emitting a Copy instruction. Otherwise, a new register
/// is allocated and the expression is translated into it.
///
/// Callers MUST use the returned register — they cannot assume a specific destination.
#[must_use = "the returned register must be used, because that is where the expression value is stored"]
pub fn resolve_expr(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    resolver: &Resolver,
) -> Result<usize> {
    if let Some((reg, needs_decode, _collation)) = resolver.resolve_cached_expr_reg(expr) {
        if !needs_decode {
            return Ok(reg);
        }
    }
    let dest_reg = program.alloc_register();
    translate_expr(program, referenced_tables, expr, dest_reg, resolver)
}

pub(super) type TranslateExprContinuation<'a, 'r> =
    Box<dyn for<'p> FnOnce(&mut IterativeExprTranslator<'p, 'a, 'r>) -> Result<()> + 'a>;

pub(super) enum TranslateExprWork<'a, 'r> {
    Visit {
        referenced_tables: Option<&'a TableReferences>,
        expr: &'a ast::Expr,
        target_register: usize,
    },
    ConditionVisit {
        referenced_tables: &'a TableReferences,
        expr: &'a ast::Expr,
        condition_metadata: ConditionMetadata,
    },
    Continue(TranslateExprContinuation<'a, 'r>),
    EnterSelfTableContext(SelfTableContext),
    ExitSelfTableContext,
    PreassignLabel(BranchOffset),
}

pub(super) struct LocalCachedExprReg<'a> {
    expr: &'a ast::Expr,
    reg: usize,
    count: usize,
    needs_decode: bool,
    collation: CachedExprCollation,
}

#[derive(Clone, Copy)]
pub(super) struct IifScheduleContext<'a> {
    referenced_tables: Option<&'a TableReferences>,
    args: &'a [Box<ast::Expr>],
    target_register: usize,
    condition_reg: usize,
    end_label: BranchOffset,
    constant_span: Option<usize>,
}

pub(super) struct TypeExprParam<'a> {
    name: String,
    expr: &'a ast::Expr,
}

pub(super) struct IterativeExprTranslator<'p, 'a, 'r> {
    program: &'p mut ProgramBuilder,
    resolver: &'a Resolver<'r>,
    work: Vec<TranslateExprWork<'a, 'r>>,
    local_expr_cache: Vec<LocalCachedExprReg<'a>>,
    self_table_context_stack: Vec<Option<SelfTableContext>>,
}

impl<'p, 'a, 'r> IterativeExprTranslator<'p, 'a, 'r> {
    pub(super) fn new(program: &'p mut ProgramBuilder, resolver: &'a Resolver<'r>) -> Self {
        Self {
            program,
            resolver,
            work: Vec::new(),
            local_expr_cache: Vec::new(),
            self_table_context_stack: Vec::new(),
        }
    }

    fn translate(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        expr: &'a ast::Expr,
        target_register: usize,
    ) -> Result<usize> {
        self.visit(referenced_tables, expr, target_register);
        self.run()?;
        Ok(target_register)
    }

    fn visit(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        expr: &'a ast::Expr,
        target_register: usize,
    ) {
        self.work.push(TranslateExprWork::Visit {
            referenced_tables,
            expr,
            target_register,
        });
    }

    fn push_translate_no_constant_opt(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        expr: &'a ast::Expr,
        target_register: usize,
    ) {
        let next_span_idx = self.program.constant_spans_next_idx();
        self.act(move |translator| {
            translator
                .program
                .constant_spans_invalidate_after(next_span_idx);
            Ok(())
        });
        self.visit(referenced_tables, expr, target_register);
    }

    fn push_translate_no_constant_opt_deferred(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        expr: &'a ast::Expr,
        target_register: usize,
    ) {
        self.act(move |translator| {
            translator.push_translate_no_constant_opt(referenced_tables, expr, target_register);
            Ok(())
        });
    }

    fn push_translate_args(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        args: &'a [Box<ast::Expr>],
        start_reg: usize,
    ) {
        for (i, arg) in args.iter().enumerate().rev() {
            self.visit(referenced_tables, arg, start_reg + i);
        }
    }

    pub(super) fn schedule_condition_expr(
        &mut self,
        referenced_tables: &'a TableReferences,
        expr: &'a ast::Expr,
        condition_metadata: ConditionMetadata,
    ) {
        self.work.push(TranslateExprWork::ConditionVisit {
            referenced_tables,
            expr,
            condition_metadata,
        });
    }

    fn push_preassign_label(&mut self, label: BranchOffset) {
        self.work.push(TranslateExprWork::PreassignLabel(label));
    }

    fn push_finish_constant_span(&mut self, constant_span: Option<usize>) {
        self.act(move |translator| {
            translator.finish_constant_span(constant_span);
            Ok(())
        });
    }

    fn schedule_expr_condition_jump(
        &mut self,
        referenced_tables: &'a TableReferences,
        expr: &'a ast::Expr,
        condition_metadata: ConditionMetadata,
    ) {
        let reg = self.program.alloc_register();
        self.act(move |translator| {
            emit_cond_jump(translator.program, condition_metadata, reg);
            Ok(())
        });
        self.visit(Some(referenced_tables), expr, reg);
    }

    fn act(
        &mut self,
        continuation: impl for<'x> FnOnce(&mut IterativeExprTranslator<'x, 'a, 'r>) -> Result<()> + 'a,
    ) {
        self.work
            .push(TranslateExprWork::Continue(Box::new(continuation)));
    }

    pub(super) fn schedule_type_expr(
        &mut self,
        expr: &'a ast::Expr,
        value_reg: usize,
        dest_reg: usize,
        column: &'a Column,
        type_def: &'a TypeDef,
    ) -> Result<usize> {
        let params = self.type_expr_params(column, type_def);
        Ok(self.schedule_type_expr_with_params(expr, value_reg, dest_reg, params))
    }

    fn schedule_type_expr_with_params(
        &mut self,
        expr: &'a ast::Expr,
        value_reg: usize,
        dest_reg: usize,
        params: Vec<TypeExprParam<'a>>,
    ) -> usize {
        self.act(|translator| {
            translator.program.id_register_overrides.clear();
            Ok(())
        });
        self.push_translate_no_constant_opt_deferred(None, expr, dest_reg);

        // Set up type parameter overrides. Capture the result so we can
        // clean up overrides even if param translation fails.
        for param in params.into_iter().rev() {
            let reg = self.program.alloc_register();
            let TypeExprParam { name, expr } = param;
            self.act(move |translator| {
                translator.program.id_register_overrides.insert(name, reg);
                Ok(())
            });
            self.visit(None, expr, reg);
        }

        // Translate the expression, disabling constant optimization since
        // the `value` placeholder refers to a register that changes per row.
        self.act(move |translator| {
            translator
                .program
                .id_register_overrides
                .insert("value".to_string(), value_reg);
            Ok(())
        });
        dest_reg
    }

    fn type_expr_params(
        &self,
        column: &'a Column,
        type_def: &'a TypeDef,
    ) -> Vec<TypeExprParam<'a>> {
        type_def
            .user_params()
            .enumerate()
            .filter_map(|(i, param)| {
                column.ty_params.get(i).map(|expr| TypeExprParam {
                    name: param.name.clone(),
                    expr: expr.as_ref(),
                })
            })
            .collect()
    }

    fn type_expr_params_from_exprs(
        &self,
        type_def: &'a TypeDef,
        ty_params: Vec<&'a ast::Expr>,
    ) -> Vec<TypeExprParam<'a>> {
        // Skip `value` param (already handled above); match remaining params
        // against the user-provided ty_params by position.
        type_def
            .user_params()
            .enumerate()
            .filter_map(|(i, param)| {
                ty_params.get(i).copied().map(|expr| TypeExprParam {
                    name: param.name.clone(),
                    expr,
                })
            })
            .collect()
    }

    fn ast_type_params(&self, tn: &'a ast::Type) -> Vec<&'a ast::Expr> {
        match &tn.size {
            Some(ast::TypeSize::MaxSize(e)) => vec![e.as_ref()],
            Some(ast::TypeSize::TypeSize(e1, e2)) => vec![e1.as_ref(), e2.as_ref()],
            None => Vec::new(),
        }
    }

    fn resolve_type_chain(
        &self,
        type_name: &str,
        is_strict: bool,
    ) -> Result<Option<Vec<&'a TypeDef>>> {
        let Some(resolved) = self.resolver.schema().resolve_type(type_name, is_strict)? else {
            return Ok(None);
        };
        Ok(Some(
            resolved
                .chain
                .iter()
                .map(|td| {
                    self.resolver
                        .schema()
                        .get_type_def_unchecked(&td.name)
                        .expect("resolved type should exist")
                        .as_ref()
                })
                .collect(),
        ))
    }

    fn resolve_type_chain_unchecked(&self, type_name: &str) -> Result<Option<Vec<&'a TypeDef>>> {
        let Some(resolved) = self.resolver.schema().resolve_type_unchecked(type_name)? else {
            return Ok(None);
        };
        Ok(Some(
            resolved
                .chain
                .iter()
                .map(|td| {
                    self.resolver
                        .schema()
                        .get_type_def_unchecked(&td.name)
                        .expect("resolved type should exist")
                        .as_ref()
                })
                .collect(),
        ))
    }

    fn schedule_user_facing_column_value(
        &mut self,
        source_reg: usize,
        dest_reg: usize,
        column: &'a Column,
        is_strict: bool,
    ) -> Result<()> {
        if source_reg != dest_reg {
            self.program.emit_insn(Insn::Copy {
                src_reg: source_reg,
                dst_reg: dest_reg,
                extra_amount: 0,
            });
        }

        if column.is_array() {
            return Ok(());
        }

        // Hash join payloads store raw encoded values; apply DECODE for custom
        // type columns so the result set contains human-readable text.
        let Some(chain) = self.resolve_type_chain(&column.ty_str, is_strict)? else {
            return Ok(());
        };

        let skip_label = self.program.allocate_label();
        self.program.emit_insn(Insn::IsNull {
            reg: dest_reg,
            target_pc: skip_label,
        });

        self.push_preassign_label(skip_label);
        for td in chain {
            if let Some(decode_expr) = td.decode() {
                self.schedule_type_expr(decode_expr, dest_reg, dest_reg, column, td)?;
            }
        }
        Ok(())
    }

    fn push_enter_self_table_context(&mut self, ctx: SelfTableContext) {
        self.work
            .push(TranslateExprWork::EnterSelfTableContext(ctx));
    }

    fn push_exit_self_table_context(&mut self) {
        self.work.push(TranslateExprWork::ExitSelfTableContext);
    }

    fn push_local_expr_cache(
        &mut self,
        expr: &'a ast::Expr,
        reg: usize,
        count: usize,
        needs_decode: bool,
        collation: CachedExprCollation,
    ) {
        self.local_expr_cache.push(LocalCachedExprReg {
            expr,
            reg,
            count,
            needs_decode,
            collation,
        });
    }

    fn pop_local_expr_cache(&mut self) {
        self.local_expr_cache
            .pop()
            .expect("local expression cache should be balanced");
    }

    pub(super) fn run(&mut self) -> Result<()> {
        while let Some(work) = self.work.pop() {
            match work {
                TranslateExprWork::Visit {
                    referenced_tables,
                    expr,
                    target_register,
                } => self.visit_expr(referenced_tables, expr, target_register)?,
                TranslateExprWork::ConditionVisit {
                    referenced_tables,
                    expr,
                    condition_metadata,
                } => self.visit_condition_expr(referenced_tables, expr, condition_metadata)?,
                TranslateExprWork::Continue(continuation) => continuation(self)?,
                TranslateExprWork::EnterSelfTableContext(ctx) => {
                    let prev = self.program.replace_self_table_context(Some(ctx));
                    self.self_table_context_stack.push(prev);
                }
                TranslateExprWork::ExitSelfTableContext => {
                    let prev = self
                        .self_table_context_stack
                        .pop()
                        .expect("self-table context stack should be balanced");
                    self.program.replace_self_table_context(prev);
                }
                TranslateExprWork::PreassignLabel(label) => {
                    self.program.preassign_label_to_next_insn(label);
                }
            }
        }
        Ok(())
    }

    fn visit_condition_expr(
        &mut self,
        referenced_tables: &'a TableReferences,
        expr: &'a ast::Expr,
        condition_metadata: ConditionMetadata,
    ) -> Result<()> {
        match expr {
            ast::Expr::Binary(lhs, ast::Operator::And, rhs) => {
                // In a binary AND, never jump to the parent 'jump_target_when_true' label on the first condition, because
                // the second condition MUST also be true. Instead we instruct the child expression to jump to a local
                // true label.
                let jump_target_when_true = self.program.allocate_label();
                self.schedule_condition_expr(referenced_tables, rhs, condition_metadata);
                self.push_preassign_label(jump_target_when_true);
                self.schedule_condition_expr(
                    referenced_tables,
                    lhs,
                    ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        ..condition_metadata
                    },
                );
            }
            ast::Expr::Binary(lhs, ast::Operator::Or, rhs) => {
                // In a binary OR, never jump to the parent 'jump_target_when_false' or
                // 'jump_target_when_null' label on the first condition, because the second
                // condition CAN also be true. Instead we instruct the child expression to
                // jump to a local false label so the right side of OR gets evaluated.
                // This is critical for cases like `x IN (NULL, 3) OR b` where the left side
                // evaluates to NULL — we must still evaluate the right side.
                let jump_target_when_false = self.program.allocate_label();
                self.schedule_condition_expr(referenced_tables, rhs, condition_metadata);
                self.push_preassign_label(jump_target_when_false);
                self.schedule_condition_expr(
                    referenced_tables,
                    lhs,
                    ConditionMetadata {
                        jump_if_condition_is_true: true,
                        jump_target_when_false,
                        jump_target_when_null: jump_target_when_false,
                        ..condition_metadata
                    },
                );
            }
            ast::Expr::Parenthesized(exprs) => {
                if exprs.len() != 1 {
                    crate::bail_parse_error!(
                        "parenthesized conditional should have exactly one expression"
                    );
                }
                self.schedule_condition_expr(referenced_tables, &exprs[0], condition_metadata);
            }
            _ => self.visit_condition_expr_leaf(referenced_tables, expr, condition_metadata)?,
        }
        Ok(())
    }

    fn visit_condition_expr_leaf(
        &mut self,
        referenced_tables: &'a TableReferences,
        expr: &'a ast::Expr,
        condition_metadata: ConditionMetadata,
    ) -> Result<()> {
        match expr {
            ast::Expr::SubqueryResult { query_type, .. } => match query_type {
                SubqueryType::Exists { result_reg } => {
                    emit_cond_jump(self.program, condition_metadata, *result_reg);
                }
                SubqueryType::In { .. } => {
                    self.schedule_expr_condition_jump(referenced_tables, expr, condition_metadata);
                }
                SubqueryType::RowValue { num_regs, .. } => {
                    if *num_regs != 1 {
                        crate::bail_parse_error!(
                            "sub-select returns {num_regs} columns - expected 1"
                        );
                    }
                    self.schedule_expr_condition_jump(referenced_tables, expr, condition_metadata);
                }
            },
            ast::Expr::Register(_) => {
                crate::bail_parse_error!(
                    "Register in WHERE clause is currently unused. Consider removing Resolver::expr_to_reg_cache and using Expr::Register instead"
                );
            }
            ast::Expr::Collate(_, _) => {
                crate::bail_parse_error!("Collate in WHERE clause is not supported");
            }
            ast::Expr::DoublyQualified(_, _, _) | ast::Expr::Id(_) | ast::Expr::Qualified(_, _) => {
                crate::bail_parse_error!(
                    "DoublyQualified/Id/Qualified should have been rewritten to Column during binding"
                );
            }
            ast::Expr::FieldAccess { .. } => {
                crate::bail_parse_error!(
                    "struct/union field access cannot be used as a bare boolean condition in WHERE"
                );
            }
            ast::Expr::Exists(_) => {
                crate::bail_parse_error!("EXISTS in WHERE clause is not supported");
            }
            ast::Expr::Subquery(_) => {
                crate::bail_parse_error!("Subquery in WHERE clause is not supported");
            }
            ast::Expr::InSelect { .. } => {
                crate::bail_parse_error!("IN (...subquery) in WHERE clause is not supported");
            }
            ast::Expr::InTable { .. } => {
                crate::bail_parse_error!("Table expression in WHERE clause is not supported");
            }
            ast::Expr::FunctionCallStar { .. } => {
                crate::bail_parse_error!("FunctionCallStar in WHERE clause is not supported");
            }
            ast::Expr::Raise(_, _) => {
                crate::bail_parse_error!("RAISE in WHERE clause is not supported");
            }
            ast::Expr::Between { .. } | ast::Expr::Variable(_) => {
                self.schedule_expr_condition_jump(referenced_tables, expr, condition_metadata);
            }
            ast::Expr::Name(_) => {
                crate::bail_parse_error!(
                    "Name as a direct predicate in WHERE clause is not supported"
                );
            }
            ast::Expr::Binary(_, ast::Operator::Is | ast::Operator::IsNot, e2)
                if matches!(
                    e2.as_ref(),
                    ast::Expr::Literal(ast::Literal::True)
                        | ast::Expr::Literal(ast::Literal::False)
                ) =>
            {
                // Handle IS TRUE/IS FALSE/IS NOT TRUE/IS NOT FALSE in conditions
                // Delegate to translate_expr which handles these correctly with IsTrue instruction
                self.schedule_expr_condition_jump(referenced_tables, expr, condition_metadata);
            }
            ast::Expr::Binary(e1, ast::Operator::Is, e2)
                if matches!(e2.as_ref(), ast::Expr::Literal(ast::Literal::Null)) =>
            {
                // Handle IS NULL/IS NOT NULL in conditions using IsNull/NotNull opcodes.
                // "a IS NULL" is parsed as Binary(a, Is, Null), but we need to use the IsNull opcode
                // (not Eq/Ne with null_eq flag) for correct NULL handling in WHERE clauses.
                let cur_reg = self.program.alloc_register();
                self.act(move |translator| {
                    if condition_metadata.jump_if_condition_is_true {
                        translator.program.emit_insn(Insn::IsNull {
                            reg: cur_reg,
                            target_pc: condition_metadata.jump_target_when_true,
                        });
                    } else {
                        translator.program.emit_insn(Insn::NotNull {
                            reg: cur_reg,
                            target_pc: condition_metadata.jump_target_when_false,
                        });
                    }
                    Ok(())
                });
                self.visit(Some(referenced_tables), e1, cur_reg);
            }
            ast::Expr::Binary(e1, ast::Operator::IsNot, e2)
                if matches!(e2.as_ref(), ast::Expr::Literal(ast::Literal::Null)) =>
            {
                let cur_reg = self.program.alloc_register();
                self.act(move |translator| {
                    if condition_metadata.jump_if_condition_is_true {
                        translator.program.emit_insn(Insn::NotNull {
                            reg: cur_reg,
                            target_pc: condition_metadata.jump_target_when_true,
                        });
                    } else {
                        translator.program.emit_insn(Insn::IsNull {
                            reg: cur_reg,
                            target_pc: condition_metadata.jump_target_when_false,
                        });
                    }
                    Ok(())
                });
                self.visit(Some(referenced_tables), e1, cur_reg);
            }
            ast::Expr::Binary(e1, op, e2) => {
                // Check if either operand has a custom type with a matching operator.
                if find_custom_type_operator(e1, e2, op, Some(referenced_tables), self.resolver)
                    .is_some()
                {
                    self.schedule_expr_condition_jump(referenced_tables, expr, condition_metadata);
                } else {
                    let result_reg = self.program.alloc_register();
                    self.schedule_binary_expr_shared(
                        Some(referenced_tables),
                        e1,
                        e2,
                        *op,
                        result_reg,
                        BinaryEmitMode::Condition(condition_metadata),
                        None,
                    )?;
                }
            }
            ast::Expr::Literal(_)
            | ast::Expr::Cast { .. }
            | ast::Expr::FunctionCall { .. }
            | ast::Expr::Column { .. }
            | ast::Expr::RowId { .. }
            | ast::Expr::Case { .. } => {
                self.schedule_expr_condition_jump(referenced_tables, expr, condition_metadata);
            }
            ast::Expr::InList { lhs, not, rhs } => {
                let ConditionMetadata {
                    jump_if_condition_is_true,
                    jump_target_when_true,
                    jump_target_when_false,
                    jump_target_when_null,
                } = condition_metadata;

                let adjusted_metadata = if *not {
                    let not_true_label = self.program.allocate_label();
                    let not_false_label = self.program.allocate_label();
                    self.act(move |translator| {
                        translator
                            .program
                            .preassign_label_to_next_insn(not_true_label);
                        translator.program.emit_insn(Insn::Goto {
                            target_pc: jump_target_when_false,
                        });
                        translator
                            .program
                            .preassign_label_to_next_insn(not_false_label);
                        translator.program.emit_insn(Insn::Goto {
                            target_pc: jump_target_when_true,
                        });
                        Ok(())
                    });
                    ConditionMetadata {
                        jump_if_condition_is_true,
                        jump_target_when_true: not_true_label,
                        jump_target_when_false: not_false_label,
                        jump_target_when_null,
                    }
                } else {
                    condition_metadata
                };

                self.schedule_in_list_condition(
                    Some(referenced_tables),
                    lhs,
                    rhs,
                    adjusted_metadata,
                )?;
            }
            ast::Expr::Like { not, .. } => {
                let cur_reg = self.program.alloc_register();
                self.act(move |translator| {
                    if !*not {
                        emit_cond_jump(translator.program, condition_metadata, cur_reg);
                    } else if condition_metadata.jump_if_condition_is_true {
                        translator.program.emit_insn(Insn::IfNot {
                            reg: cur_reg,
                            target_pc: condition_metadata.jump_target_when_true,
                            jump_if_null: false,
                        });
                    } else {
                        translator.program.emit_insn(Insn::If {
                            reg: cur_reg,
                            target_pc: condition_metadata.jump_target_when_false,
                            jump_if_null: true,
                        });
                    }
                    Ok(())
                });
                self.schedule_like_base(Some(referenced_tables), expr, cur_reg)?;
            }
            ast::Expr::Parenthesized(_) => {
                unreachable!("parenthesized conditions are handled by visit_condition_expr")
            }
            ast::Expr::NotNull(expr) => {
                let cur_reg = self.program.alloc_register();
                self.act(move |translator| {
                    if condition_metadata.jump_if_condition_is_true {
                        translator.program.emit_insn(Insn::NotNull {
                            reg: cur_reg,
                            target_pc: condition_metadata.jump_target_when_true,
                        });
                    } else {
                        translator.program.emit_insn(Insn::IsNull {
                            reg: cur_reg,
                            target_pc: condition_metadata.jump_target_when_false,
                        });
                    }
                    Ok(())
                });
                self.visit(Some(referenced_tables), expr, cur_reg);
            }
            ast::Expr::IsNull(expr) => {
                let cur_reg = self.program.alloc_register();
                self.act(move |translator| {
                    if condition_metadata.jump_if_condition_is_true {
                        translator.program.emit_insn(Insn::IsNull {
                            reg: cur_reg,
                            target_pc: condition_metadata.jump_target_when_true,
                        });
                    } else {
                        translator.program.emit_insn(Insn::NotNull {
                            reg: cur_reg,
                            target_pc: condition_metadata.jump_target_when_false,
                        });
                    }
                    Ok(())
                });
                self.visit(Some(referenced_tables), expr, cur_reg);
            }
            ast::Expr::Unary(_, _) => {
                self.schedule_expr_condition_jump(referenced_tables, expr, condition_metadata);
            }
            ast::Expr::Default => {
                crate::bail_parse_error!("DEFAULT is only valid in INSERT VALUES");
            }
            ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
                unreachable!("Array and Subscript are desugared into function calls by the parser")
            }
        }
        Ok(())
    }

    fn finish_constant_span(&mut self, constant_span: Option<usize>) {
        if let Some(span) = constant_span {
            self.program.constant_span_end(span);
        }
    }

    fn visit_expr(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        expr: &'a ast::Expr,
        target_register: usize,
    ) -> Result<()> {
        let constant_span = if expr.is_constant(self.resolver) {
            if !self.program.constant_span_is_open() {
                Some(self.program.constant_span_start())
            } else {
                None
            }
        } else {
            self.program.constant_span_end_all();
            None
        };

        if self.emit_cached_expr(referenced_tables, expr, target_register, constant_span)? {
            return Ok(());
        }

        let has_expression_indexes = referenced_tables.is_some_and(|tables| {
            tables
                .joined_tables()
                .iter()
                .any(|t| !t.expression_index_usages.is_empty())
        });
        if has_expression_indexes
            && try_emit_expression_index_value(
                self.program,
                referenced_tables,
                expr,
                target_register,
            )?
        {
            self.finish_constant_span(constant_span);
            return Ok(());
        }

        match expr {
            ast::Expr::SubqueryResult {
                lhs,
                not_in,
                query_type,
                ..
            } => {
                self.schedule_subquery_result(
                    referenced_tables,
                    lhs.as_deref(),
                    *not_in,
                    query_type,
                    target_register,
                    constant_span,
                )?;
            }
            ast::Expr::Between {
                lhs,
                not,
                start,
                end,
            } => {
                self.schedule_between_expr(
                    referenced_tables,
                    lhs,
                    *not,
                    start,
                    end,
                    target_register,
                    constant_span,
                )?;
            }
            ast::Expr::Binary(e1, op, e2) => {
                self.visit_binary_expr(
                    referenced_tables,
                    e1,
                    op,
                    e2,
                    target_register,
                    constant_span,
                )?;
            }
            ast::Expr::Register(src_reg) => {
                self.program.emit_insn(Insn::Copy {
                    src_reg: *src_reg,
                    dst_reg: target_register,
                    extra_amount: 0,
                });
                self.finish_constant_span(constant_span);
            }
            ast::Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                self.schedule_case_expr(
                    referenced_tables,
                    base.as_deref(),
                    when_then_pairs,
                    else_expr.as_deref(),
                    target_register,
                    constant_span,
                );
            }
            ast::Expr::Cast { expr, type_name } => {
                self.act(move |translator| {
                    translator.schedule_cast_after_child(
                        type_name.as_ref(),
                        target_register,
                        constant_span,
                    )?;
                    Ok(())
                });
                self.visit(referenced_tables, expr, target_register);
            }
            ast::Expr::Collate(inner, collation) => {
                // First translate inner expr, then set the curr collation. If we set curr collation before,
                // it may be overwritten later by inner translate.
                self.act(move |translator| {
                    let collation = CollationSeq::new(collation.as_str())?;
                    translator.program.set_collation(Some((collation, true)));
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, inner, target_register);
            }
            ast::Expr::DoublyQualified(_, _, _) => {
                unreachable!("DoublyQualified should be resolved to a Column before translation")
            }
            ast::Expr::Exists(_) => {
                crate::bail_parse_error!("Exists is not supported in this position")
            }
            ast::Expr::FieldAccess {
                base,
                field,
                resolved,
            } => {
                self.schedule_field_access(
                    referenced_tables,
                    base,
                    field,
                    resolved.as_ref(),
                    target_register,
                    constant_span,
                );
            }
            ast::Expr::FunctionCall {
                name,
                distinctness: _,
                args,
                filter_over,
                order_by: _,
            } => {
                self.schedule_function_call(
                    referenced_tables,
                    name,
                    args,
                    filter_over,
                    target_register,
                    constant_span,
                )?;
            }
            ast::Expr::FunctionCallStar { name, filter_over } => {
                self.schedule_function_call_star(
                    referenced_tables,
                    name,
                    filter_over,
                    target_register,
                    constant_span,
                )?;
            }
            ast::Expr::Id(id) => {
                if let Some(&reg) = self.program.id_register_overrides.get(id.as_str()) {
                    self.program.emit_insn(Insn::Copy {
                        src_reg: reg,
                        dst_reg: target_register,
                        extra_amount: 0,
                    });
                } else {
                    if !self.resolver.dqs_dml.is_enabled() {
                        crate::bail_parse_error!("no such column: {}", id.as_str());
                    }
                    self.program.emit_insn(Insn::String8 {
                        value: id.as_str().to_string(),
                        dest: target_register,
                    });
                }
                self.finish_constant_span(constant_span);
            }
            ast::Expr::Column {
                table: table_ref_id,
                column,
                is_rowid_alias,
                ..
            } => {
                self.schedule_column_expr(
                    referenced_tables,
                    *table_ref_id,
                    *column,
                    *is_rowid_alias,
                    target_register,
                    constant_span,
                )?;
            }
            ast::Expr::RowId { table, .. } => {
                self.emit_rowid_expr(referenced_tables, *table, target_register)?;
                self.finish_constant_span(constant_span);
            }
            ast::Expr::InList { lhs, rhs, not } => {
                self.schedule_in_list_expr(
                    referenced_tables,
                    lhs,
                    rhs,
                    *not,
                    target_register,
                    constant_span,
                )?;
            }
            ast::Expr::InSelect { .. } => {
                crate::bail_parse_error!("IN (...subquery) is not supported in this position")
            }
            ast::Expr::InTable { .. } => {
                crate::bail_parse_error!("Table expression is not supported in this position")
            }
            ast::Expr::IsNull(inner) => {
                let reg = self.program.alloc_register();
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::Integer {
                        value: 1,
                        dest: target_register,
                    });
                    let label = translator.program.allocate_label();
                    translator.program.emit_insn(Insn::IsNull {
                        reg,
                        target_pc: label,
                    });
                    translator.program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: target_register,
                    });
                    translator.program.preassign_label_to_next_insn(label);
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, inner, reg);
            }
            ast::Expr::Literal(lit) => {
                emit_literal(self.program, lit, target_register)?;
                self.finish_constant_span(constant_span);
            }
            ast::Expr::Name(_) => {
                crate::bail_parse_error!("ast::Expr::Name is not supported in this position")
            }
            ast::Expr::Like { not, .. } => {
                let like_reg = if *not {
                    self.program.alloc_register()
                } else {
                    target_register
                };
                self.act(move |translator| {
                    if *not {
                        translator.program.emit_insn(Insn::Not {
                            reg: like_reg,
                            dest: target_register,
                        });
                    }
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.schedule_like_base(referenced_tables, expr, like_reg)?;
            }
            ast::Expr::NotNull(inner) => {
                let reg = self.program.alloc_register();
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::Integer {
                        value: 1,
                        dest: target_register,
                    });
                    let label = translator.program.allocate_label();
                    translator.program.emit_insn(Insn::NotNull {
                        reg,
                        target_pc: label,
                    });
                    translator.program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: target_register,
                    });
                    translator.program.preassign_label_to_next_insn(label);
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, inner, reg);
            }
            ast::Expr::Parenthesized(exprs) => {
                if exprs.is_empty() {
                    crate::bail_parse_error!("parenthesized expression with no arguments");
                }
                assert_register_range_allocated(self.program, target_register, exprs.len())?;
                self.act(move |translator| {
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                for (i, expr) in exprs.iter().enumerate().rev() {
                    self.visit(referenced_tables, expr, target_register + i);
                }
            }
            ast::Expr::Qualified(_, _) => {
                unreachable!("Qualified should be resolved to a Column before translation")
            }
            ast::Expr::Raise(resolve_type, msg_expr) => {
                self.schedule_raise_expr(
                    referenced_tables,
                    *resolve_type,
                    msg_expr.as_deref(),
                    target_register,
                    constant_span,
                )?;
            }
            ast::Expr::Unary(op, inner) => {
                self.visit_unary_expr(
                    referenced_tables,
                    *op,
                    inner,
                    target_register,
                    constant_span,
                )?;
            }
            ast::Expr::Subquery(_) => {
                crate::bail_parse_error!("Subquery is not supported in this position")
            }
            ast::Expr::Variable(variable) => {
                let index = self.program.register_variable(variable);
                self.program.emit_insn(Insn::Variable {
                    index,
                    dest: target_register,
                });
                self.finish_constant_span(constant_span);
            }
            ast::Expr::Default => {
                crate::bail_parse_error!("DEFAULT is only valid in INSERT VALUES");
            }
            ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
                unreachable!("Array and Subscript are desugared into function calls by the parser")
            }
        }

        Ok(())
    }

    fn emit_cached_expr(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        expr: &'a ast::Expr,
        target_register: usize,
        constant_span: Option<usize>,
    ) -> Result<bool> {
        if let Some((reg, count, needs_decode, collation_ctx)) = self
            .local_expr_cache
            .iter()
            .rev()
            .find(|entry| exprs_are_equivalent(expr, entry.expr))
            .map(|entry| (entry.reg, entry.count, entry.needs_decode, entry.collation))
        {
            self.program.emit_insn(Insn::Copy {
                src_reg: reg,
                dst_reg: target_register,
                extra_amount: count - 1,
            });
            if needs_decode && !self.program.flags.suppress_custom_type_decode() {
                if let ast::Expr::Column {
                    table: table_ref_id,
                    column,
                    ..
                } = expr
                {
                    if let Some(referenced_tables) = referenced_tables {
                        if let Some((_, table)) =
                            referenced_tables.find_table_by_internal_id(*table_ref_id)
                        {
                            if let Some(col) = table.get_column_at(*column) {
                                if let Some(type_def) = self
                                    .resolver
                                    .schema()
                                    .get_type_def(&col.ty_str, table.is_strict())
                                {
                                    if let Some(decode_expr) = type_def.decode() {
                                        let skip_label = self.program.allocate_label();
                                        self.program.emit_insn(Insn::IsNull {
                                            reg: target_register,
                                            target_pc: skip_label,
                                        });
                                        self.act(move |translator| {
                                            translator
                                                .program
                                                .preassign_label_to_next_insn(skip_label);
                                            translator.program.set_collation(collation_ctx);
                                            translator.finish_constant_span(constant_span);
                                            Ok(())
                                        });
                                        self.schedule_type_expr(
                                            decode_expr,
                                            target_register,
                                            target_register,
                                            col,
                                            type_def,
                                        )?;
                                        return Ok(true);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            self.program.set_collation(collation_ctx);
            self.finish_constant_span(constant_span);
            return Ok(true);
        }

        let Some((reg, needs_decode, collation_ctx)) = self.resolver.resolve_cached_expr_reg(expr)
        else {
            return Ok(false);
        };
        self.program.emit_insn(Insn::Copy {
            src_reg: reg,
            dst_reg: target_register,
            extra_amount: 0,
        });
        if needs_decode && !self.program.flags.suppress_custom_type_decode() {
            if let ast::Expr::Column {
                table: table_ref_id,
                column,
                ..
            } = expr
            {
                if let Some(referenced_tables) = referenced_tables {
                    if let Some((_, table)) =
                        referenced_tables.find_table_by_internal_id(*table_ref_id)
                    {
                        if let Some(col) = table.get_column_at(*column) {
                            if let Some(type_def) = self
                                .resolver
                                .schema()
                                .get_type_def(&col.ty_str, table.is_strict())
                            {
                                if let Some(decode_expr) = type_def.decode() {
                                    let skip_label = self.program.allocate_label();
                                    self.program.emit_insn(Insn::IsNull {
                                        reg: target_register,
                                        target_pc: skip_label,
                                    });
                                    self.act(move |translator| {
                                        translator.program.preassign_label_to_next_insn(skip_label);
                                        translator.program.set_collation(collation_ctx);
                                        translator.finish_constant_span(constant_span);
                                        Ok(())
                                    });
                                    self.schedule_type_expr(
                                        decode_expr,
                                        target_register,
                                        target_register,
                                        col,
                                        type_def,
                                    )?;
                                    return Ok(true);
                                }
                            }
                        }
                    }
                }
            }
        }
        self.program.set_collation(collation_ctx);
        self.finish_constant_span(constant_span);
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn schedule_subquery_result(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        lhs: Option<&'a ast::Expr>,
        not_in: bool,
        query_type: &'a SubqueryType,
        target_register: usize,
        constant_span: Option<usize>,
    ) -> Result<()> {
        match query_type {
            SubqueryType::Exists { result_reg } => {
                self.program.emit_insn(Insn::Copy {
                    src_reg: *result_reg,
                    dst_reg: target_register,
                    extra_amount: 0,
                });
                self.finish_constant_span(constant_span);
            }
            SubqueryType::RowValue {
                result_reg_start,
                num_regs,
            } => {
                assert_register_range_allocated(self.program, target_register, *num_regs)?;
                self.program.emit_insn(Insn::Copy {
                    src_reg: *result_reg_start,
                    dst_reg: target_register,
                    extra_amount: num_regs - 1,
                });
                self.finish_constant_span(constant_span);
            }
            SubqueryType::In {
                cursor_id,
                affinity_str,
            } => {
                let Some(lhs) = lhs else {
                    crate::bail_parse_error!("IN subquery result missing left-hand side");
                };
                let label_skip_row = self.program.allocate_label();
                let label_include_row = self.program.allocate_label();
                let label_null_result = self.program.allocate_label();
                let label_null_rewind = self.program.allocate_label();
                let label_null_checks_loop_start = self.program.allocate_label();
                let label_null_checks_next = self.program.allocate_label();
                self.program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: target_register,
                });

                let lhs_columns: Vec<&'a ast::Expr> = match unwrap_parens(lhs)? {
                    ast::Expr::Parenthesized(exprs) => {
                        exprs.iter().map(std::convert::AsRef::as_ref).collect()
                    }
                    expr => vec![expr],
                };
                let lhs_column_count = lhs_columns.len();
                let lhs_column_regs_start = self.program.alloc_registers(lhs_column_count);
                let cursor_id = *cursor_id;
                let affinity_str = Arc::clone(affinity_str);

                self.act(move |translator| {
                    // Only emit Affinity instruction if there's meaningful affinity to apply
                    // (i.e., not all BLOB/NONE affinity).
                    if affinity_str
                        .chars()
                        .map(Affinity::from_char)
                        .any(|a| a != Affinity::Blob)
                    {
                        if let Ok(count) = std::num::NonZeroUsize::try_from(lhs_column_count) {
                            translator.program.emit_insn(Insn::Affinity {
                                start_reg: lhs_column_regs_start,
                                count,
                                affinities: affinity_str.as_ref().clone(),
                            });
                        }
                    }

                    let label_on_no_null = if not_in {
                        label_include_row
                    } else {
                        label_skip_row
                    };

                    // For NOT IN: empty ephemeral or no all-NULL row means TRUE (include)
                    // For IN: empty ephemeral or no all-NULL row means FALSE (skip)
                    if not_in {
                        translator.program.emit_insn(Insn::Found {
                            cursor_id,
                            target_pc: label_skip_row,
                            record_reg: lhs_column_regs_start,
                            num_regs: lhs_column_count,
                        });
                    } else {
                        translator.program.emit_insn(Insn::NotFound {
                            cursor_id,
                            target_pc: label_null_rewind,
                            record_reg: lhs_column_regs_start,
                            num_regs: lhs_column_count,
                        });
                        translator.program.emit_insn(Insn::Goto {
                            target_pc: label_include_row,
                        });
                    }

                    translator
                        .program
                        .preassign_label_to_next_insn(label_null_rewind);
                    translator.program.emit_insn(Insn::Rewind {
                        cursor_id,
                        pc_if_empty: label_on_no_null,
                    });
                    // Null checking loop: scan ephemeral for any all-NULL tuples.
                    // If found, result is NULL (unknown). If not found, result depends on IN vs NOT IN.
                    translator
                        .program
                        .preassign_label_to_next_insn(label_null_checks_loop_start);
                    let column_check_reg = translator.program.alloc_register();
                    for (i, affinity) in affinity_str.chars().map(Affinity::from_char).enumerate() {
                        translator.program.emit_insn(Insn::Column {
                            cursor_id,
                            column: i,
                            dest: column_check_reg,
                            default: None,
                        });
                        translator.program.emit_insn(Insn::Ne {
                            lhs: lhs_column_regs_start + i,
                            rhs: column_check_reg,
                            target_pc: label_null_checks_next,
                            flags: CmpInsFlags::default().with_affinity(affinity),
                            collation: translator.program.curr_collation(),
                        });
                    }
                    translator.program.emit_insn(Insn::Goto {
                        target_pc: label_null_result,
                    });
                    translator
                        .program
                        .preassign_label_to_next_insn(label_null_checks_next);
                    translator.program.emit_insn(Insn::Next {
                        cursor_id,
                        pc_if_next: label_null_checks_loop_start,
                    });
                    translator.program.emit_insn(Insn::Goto {
                        target_pc: label_on_no_null,
                    });

                    // Final result handling:
                    // label_include_row: result = 1 (TRUE)
                    // label_skip_row: result = 0 (FALSE)
                    // label_null_result: result = NULL (unknown)
                    let label_done = translator.program.allocate_label();
                    translator
                        .program
                        .preassign_label_to_next_insn(label_include_row);
                    translator.program.emit_insn(Insn::Integer {
                        value: 1,
                        dest: target_register,
                    });
                    translator.program.emit_insn(Insn::Goto {
                        target_pc: label_done,
                    });
                    translator
                        .program
                        .preassign_label_to_next_insn(label_skip_row);
                    translator.program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: target_register,
                    });
                    translator.program.emit_insn(Insn::Goto {
                        target_pc: label_done,
                    });
                    translator
                        .program
                        .preassign_label_to_next_insn(label_null_result);
                    translator.program.emit_insn(Insn::Null {
                        dest: target_register,
                        dest_end: None,
                    });
                    translator.program.preassign_label_to_next_insn(label_done);
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });

                for (i, lhs_column) in lhs_columns.iter().enumerate().rev() {
                    let reg = lhs_column_regs_start + i;
                    self.act(move |translator| {
                        // If LHS is NULL, we need to check if ephemeral is empty first.
                        // - If empty: IN returns FALSE, NOT IN returns TRUE
                        // - If not empty: result is NULL (unknown)
                        // Jump to label_null_rewind which does Rewind and handles empty case.
                        //
                        // Always emit this check even for NOT NULL columns because NullRow
                        // (used in ungrouped aggregates when no rows match) overrides all
                        // column values to NULL regardless of the NOT NULL constraint.
                        translator.program.emit_insn(Insn::IsNull {
                            reg,
                            target_pc: label_null_rewind,
                        });
                        Ok(())
                    });
                    self.visit(referenced_tables, lhs_column, reg);
                }
            }
        }
        Ok(())
    }

    fn emit_rowid_expr(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        table_ref_id: TableInternalId,
        target_register: usize,
    ) -> Result<()> {
        let referenced_tables =
            referenced_tables.expect("table_references needed translating Expr::RowId");
        let (_, table) = referenced_tables
            .find_table_by_internal_id(table_ref_id)
            .expect("table reference should be found");
        let Table::BTree(btree) = table else {
            crate::bail_parse_error!("no such column: rowid");
        };
        if !btree.has_rowid {
            crate::bail_parse_error!("no such column: rowid");
        }

        let has_cursor_override = self.program.has_cursor_override(table_ref_id);
        let (index, use_covering_index) = if has_cursor_override {
            (None, false)
        } else if let Some(table_reference) =
            referenced_tables.find_joined_table_by_internal_id(table_ref_id)
        {
            (
                table_reference.op.index(),
                table_reference.utilizes_covering_index(),
            )
        } else {
            (None, false)
        };

        if use_covering_index {
            let index = index.expect("index cursor should be opened when use_covering_index=true");
            let cursor_id = self
                .program
                .resolve_cursor_id(&CursorKey::index(table_ref_id, index.clone()));
            self.program.emit_insn(Insn::IdxRowId {
                cursor_id,
                dest: target_register,
            });
        } else {
            let cursor_id = self
                .program
                .resolve_cursor_id(&CursorKey::table(table_ref_id));
            self.program.emit_insn(Insn::RowId {
                cursor_id,
                dest: target_register,
            });
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn schedule_column_expr(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        table_ref_id: TableInternalId,
        column_idx: usize,
        is_rowid_alias: bool,
        target_register: usize,
        constant_span: Option<usize>,
    ) -> Result<()> {
        if table_ref_id.is_self_table() {
            // the table is a SELF_TABLE placeholder (used for generated columns), so we now have
            // to resolve it to the actual reference id using the SelfTableContext.
            let self_table_context = self.program.current_self_table_context().cloned();
            match self_table_context {
                Some(SelfTableContext::ForSelect {
                    table_ref_id: real_id,
                    ..
                }) => {
                    let referenced_tables = referenced_tables.ok_or_else(|| {
                        LimboError::ParseError(
                            "SELF_TABLE select context requires table references".to_string(),
                        )
                    })?;
                    return self.schedule_column_expr(
                        Some(referenced_tables),
                        real_id,
                        column_idx,
                        is_rowid_alias,
                        target_register,
                        constant_span,
                    );
                }
                Some(SelfTableContext::ForDML { dml_ctx, .. }) => {
                    let src_reg = dml_ctx.to_column_reg(column_idx);
                    self.program.emit_insn(Insn::Copy {
                        src_reg,
                        dst_reg: target_register,
                        extra_amount: 0,
                    });
                    self.finish_constant_span(constant_span);
                    return Ok(());
                }
                None => {
                    // This error means that a program.with_self_table_context() was missing
                    // somewhere in the call stack.
                    crate::bail_parse_error!(
                        "SELF_TABLE column reference outside of generated column context"
                    );
                }
            }
        }

        let has_cursor_override = self.program.has_cursor_override(table_ref_id);
        let referenced_tables =
            referenced_tables.expect("table_references needed translating Expr::Column");
        let (index, index_method, use_covering_index) = {
            if has_cursor_override {
                // When a cursor override is active for this table, we bypass all index logic
                // and read directly from the override cursor. This is used during hash join
                // build phases where we iterate using a separate cursor and don't want to use any index.
                (None, None, false)
            } else if let Some(table_reference) =
                referenced_tables.find_joined_table_by_internal_id(table_ref_id)
            {
                (
                    table_reference.op.index(),
                    if let Operation::IndexMethodQuery(index_method) = &table_reference.op {
                        Some(index_method)
                    } else {
                        None
                    },
                    table_reference.utilizes_covering_index(),
                )
            } else {
                (None, None, false)
            }
        };
        let use_index_method = index_method.and_then(|m| m.covered_columns.get(&column_idx));

        let (is_from_outer_query_scope, table) = referenced_tables
            .find_table_by_internal_id(table_ref_id)
            .unwrap_or_else(|| {
                unreachable!(
                    "table reference should be found: {} (referenced_tables: {:?})",
                    table_ref_id, referenced_tables
                )
            });

        if use_index_method.is_none() {
            let Some(table_column) = table.get_column_at(column_idx) else {
                crate::bail_parse_error!("column index out of bounds");
            };
            // Counter intuitive but a column always needs to have a collation
            self.program
                .set_collation(Some((table_column.collation(), false)));
        }

        match table {
            Table::BTree(_) => {
                // If we are reading a column from a table, we find the cursor that corresponds to
                // the table and read the column from the cursor.
                // If we have a covering index, we don't have an open table cursor so we read from the index cursor.
                let (table_cursor_id, index_cursor_id) = if is_from_outer_query_scope {
                    // Due to a limitation of our translation system, a subquery that references an outer query table
                    // cannot know whether a table cursor, index cursor, or both were opened for that table reference.
                    // Hence: currently we first try to resolve a table cursor, and if that fails,
                    // we resolve an index cursor.
                    if let Some(table_cursor_id) = self
                        .program
                        .resolve_cursor_id_safe(&CursorKey::table(table_ref_id))
                    {
                        (Some(table_cursor_id), None)
                    } else {
                        (
                            None,
                            Some(
                                self.program
                                    .resolve_any_index_cursor_id_for_table(table_ref_id),
                            ),
                        )
                    }
                } else {
                    let table_cursor_id = if use_covering_index || use_index_method.is_some() {
                        None
                    } else {
                        Some(
                            self.program
                                .resolve_cursor_id(&CursorKey::table(table_ref_id)),
                        )
                    };
                    let index_cursor_id = index.map(|index| {
                        self.program
                            .resolve_cursor_id(&CursorKey::index(table_ref_id, index.clone()))
                    });
                    (table_cursor_id, index_cursor_id)
                };

                if let Some(custom_module_column) = use_index_method {
                    self.program.emit_column_or_rowid(
                        index_cursor_id.expect("index cursor should be opened"),
                        *custom_module_column,
                        target_register,
                    );
                } else if is_rowid_alias {
                    if let Some(index_cursor_id) = index_cursor_id {
                        self.program.emit_insn(Insn::IdxRowId {
                            cursor_id: index_cursor_id,
                            dest: target_register,
                        });
                    } else if let Some(table_cursor_id) = table_cursor_id {
                        self.program.emit_insn(Insn::RowId {
                            cursor_id: table_cursor_id,
                            dest: target_register,
                        });
                    } else {
                        unreachable!("Either index or table cursor must be opened");
                    }
                } else {
                    let is_btree_index = index_cursor_id.is_some_and(|cid| {
                        self.program
                            .get_cursor_type(cid)
                            .is_some_and(|ct| ct.is_index())
                    });
                    // FIXME(https://github.com/tursodatabase/turso/issues/4801):
                    // This is a defensive workaround for cursor desynchronization.
                    //
                    // When `use_covering_index` is false, both table AND index cursors
                    // are open and positioned at the same row. If we read some columns
                    // from the index cursor and others from the table cursor, we rely
                    // on both cursors staying synchronized.
                    //
                    // The problem: AFTER triggers can INSERT into the same table,
                    // which modifies the index btree. This repositions or invalidates
                    // the parent program's index cursor, while the table cursor remains
                    // at the correct position. Result: we read a mix of data from
                    // different rows - corruption.
                    //
                    // Why does the table cursor not have this problem? Because it's
                    // explicitly re-sought by rowid (via NotExists instruction) before
                    // each use. The rowid is stored in a register and used as a stable
                    // key. The index cursor, by contrast, just trusts its internal
                    // position (page + cell index) without re-seeking.
                    //
                    // Why not check if the table has triggers and allow the optimization
                    // when there are none? Several reasons:
                    // 1. ProgramBuilder.trigger indicates if THIS program is a trigger
                    //    subprogram, not whether the table has triggers.
                    // 2. In translate_expr(), we lack context about which table is being
                    //    modified or whether we're even in an UPDATE/INSERT/DELETE.
                    // 3. Triggers can be recursive (trigger on T inserts into U, whose
                    //    trigger inserts back into T).
                    //
                    // The proper fix is to implement SQLite's `saveAllCursors()` approach:
                    // before ANY btree write, find all cursors pointing to that btree
                    // (by root_page) and save their positions. When those cursors are
                    // next accessed, they re-seek to their saved position. This could
                    // be done lazily with a generation number per btree; cursors check
                    // if the generation changed and re-seek if needed. This would
                    // require a global cursor registry and significant refactoring.
                    //
                    // For now, we only read from the index cursor when `use_covering_index`
                    // is true, meaning only the index cursor exists (no table cursor to
                    // get out of sync with). This foregoes the optimization of reading
                    // individual columns from a non-covering index.
                    let read_from_index = if is_from_outer_query_scope {
                        is_btree_index
                    } else if is_btree_index && use_covering_index {
                        index.as_ref().is_some_and(|idx| {
                            idx.column_table_pos_to_index_pos(column_idx).is_some()
                        })
                    } else {
                        false
                    };

                    let Some(table_column) = table.get_column_at(column_idx) else {
                        crate::bail_parse_error!("column index out of bounds");
                    };
                    // if we're reading from an index that contains this virtual column,
                    // the index already has the computed value, so read it from the index
                    match table_column.generated_type() {
                        GeneratedType::Virtual { expr, .. } if !read_from_index => {
                            let table_column_affinity = table_column.affinity();
                            let table_column_collation = table_column.collation();
                            let read_cursor = table_cursor_id.or(index_cursor_id);
                            self.act(move |translator| {
                                translator
                                    .program
                                    .emit_column_affinity(target_register, table_column_affinity);
                                // The virtual column's declared collation must override
                                // whatever collation the inner expression resolved to.
                                translator
                                    .program
                                    .set_collation(Some((table_column_collation, false)));
                                translator.finish_btree_column_value(
                                    referenced_tables,
                                    table,
                                    column_idx,
                                    target_register,
                                    true,
                                    read_cursor,
                                    constant_span,
                                )
                            });
                            self.push_exit_self_table_context();
                            self.visit(Some(referenced_tables), expr, target_register);
                            self.push_enter_self_table_context(SelfTableContext::ForSelect {
                                table_ref_id,
                                referenced_tables: referenced_tables.clone(),
                            });
                            return Ok(());
                        }
                        _ => {
                            let read_cursor = if read_from_index {
                                index_cursor_id.expect("index cursor should be opened")
                            } else {
                                table_cursor_id
                                    .or(index_cursor_id)
                                    .expect("cursor should be opened")
                            };
                            let column = if read_from_index {
                                let index = self.program.resolve_index_for_cursor_id(
                                    index_cursor_id.expect("index cursor should be opened"),
                                );
                                index
                                    .column_table_pos_to_index_pos(column_idx)
                                    .unwrap_or_else(|| {
                                        panic!(
                                            "index {} does not contain column number {} of table {}",
                                            index.name, column_idx, table_ref_id
                                        )
                                    })
                            } else {
                                column_idx
                            };

                            if let Some(col) = table.get_column_at(column) {
                                if col.default.is_some() {
                                    if let Ok(Some(resolved)) = self
                                        .resolver
                                        .schema()
                                        .resolve_type(&col.ty_str, table.is_strict())
                                    {
                                        if resolved.chain.iter().any(|td| td.encode().is_some()) {
                                            // For custom type columns with ENCODE/DECODE and a
                                            // default, suppress the Column instruction's default.
                                            // We handle short records (ALTER TABLE ADD COLUMN) via
                                            // ColumnHasField after the Column instruction.
                                            self.program.flags.set_suppress_column_default(true);
                                        }
                                    }
                                }
                            }
                            self.program
                                .emit_column_or_rowid(read_cursor, column, target_register);
                        }
                    }

                    let virtual_already_applied =
                        table_column.is_virtual_generated() && !read_from_index;
                    let read_cursor = if read_from_index {
                        index_cursor_id
                    } else {
                        table_cursor_id.or(index_cursor_id)
                    };
                    self.finish_btree_column_value(
                        referenced_tables,
                        table,
                        column_idx,
                        target_register,
                        virtual_already_applied,
                        read_cursor,
                        constant_span,
                    )?;
                    return Ok(());
                }
            }
            Table::FromClauseSubquery(from_clause_subquery) => {
                if is_from_outer_query_scope {
                    // For outer-scope references during table-backed materialized-subquery
                    // seeks, read from the auxiliary index cursor: coroutine result
                    // registers are not refreshed while the seek path is iterating.
                    if let Some(cursor_id) = self
                        .program
                        .resolve_any_index_cursor_id_for_table_safe(table_ref_id)
                    {
                        let index = self.program.resolve_index_for_cursor_id(cursor_id);
                        // Read from the index cursor. Index columns may be reordered
                        // (key columns first), so find the index column position that
                        // corresponds to the original subquery column position.
                        let idx_col = index
                            .columns
                            .iter()
                            .position(|c| c.pos_in_table == column_idx)
                            .expect("index column not found for subquery column");
                        self.program.emit_insn(Insn::Column {
                            cursor_id,
                            column: idx_col,
                            dest: target_register,
                            default: None,
                        });
                        if let Some(col) = from_clause_subquery.columns.get(column_idx) {
                            maybe_apply_affinity(col.ty(), target_register, self.program);
                        }
                        self.finish_constant_span(constant_span);
                        return Ok(());
                    }
                }

                if let Some(table_reference) = referenced_tables
                    .joined_tables()
                    .iter()
                    .find(|t| t.internal_id == table_ref_id)
                {
                    // Check if this subquery was materialized with an ephemeral index.
                    // If so, read from the index cursor; otherwise copy from result registers.
                    if let Operation::Search(Search::Seek {
                        index: Some(index), ..
                    }) = &table_reference.op
                    {
                        if index.ephemeral {
                            // Read from the index cursor. Index columns may be reordered
                            // (key columns first), so find the index column position that
                            // corresponds to the original subquery column position.
                            let idx_col = index
                                .columns
                                .iter()
                                .position(|c| c.pos_in_table == column_idx)
                                .expect("index column not found for subquery column");
                            let cursor_id = self
                                .program
                                .resolve_cursor_id(&CursorKey::index(table_ref_id, index.clone()));
                            self.program.emit_insn(Insn::Column {
                                cursor_id,
                                column: idx_col,
                                dest: target_register,
                                default: None,
                            });
                            if let Some(col) = from_clause_subquery.columns.get(column_idx) {
                                maybe_apply_affinity(col.ty(), target_register, self.program);
                            }
                            self.finish_constant_span(constant_span);
                            return Ok(());
                        }
                    }
                }

                let result_columns_start = if is_from_outer_query_scope {
                    self.program.get_subquery_result_reg(table_ref_id).expect(
                        "Outer query subquery result_columns_start_reg must be set in program",
                    )
                } else {
                    from_clause_subquery
                        .result_columns_start_reg
                        .expect("Subquery result_columns_start_reg must be set")
                };
                // Fallback: copy from result registers (coroutine-based subquery).
                // For outer query subqueries, look up the register from the program builder
                // since the cloned subquery does not have the register set yet.
                self.program.emit_insn(Insn::Copy {
                    src_reg: result_columns_start + column_idx,
                    dst_reg: target_register,
                    extra_amount: 0,
                });
            }
            Table::Virtual(_) => {
                let cursor_id = self
                    .program
                    .resolve_cursor_id(&CursorKey::table(table_ref_id));
                self.program.emit_insn(Insn::VColumn {
                    cursor_id,
                    column: column_idx,
                    dest: target_register,
                });
            }
        }

        self.finish_constant_span(constant_span);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_btree_column_value(
        &mut self,
        referenced_tables: &'a TableReferences,
        table: &'a Table,
        column_idx: usize,
        target_register: usize,
        virtual_already_applied: bool,
        read_cursor: Option<CursorID>,
        constant_span: Option<usize>,
    ) -> Result<()> {
        let Some(column) = table.get_column_at(column_idx) else {
            crate::bail_parse_error!("column index out of bounds");
        };
        let table_is_strict = table.is_strict();
        let type_def = self
            .resolver
            .schema()
            .get_type_def(&column.ty_str, table_is_strict);
        // Skip affinity for custom types — the stored value is
        // already in BASE type format; the custom type name may
        // produce wrong affinity (e.g. "doubled" → REAL due to "DOUB").
        //
        // Also skip for virtual columns without a stored index value,
        // we already applied affinity for these.
        if !(virtual_already_applied || type_def.is_some()) {
            maybe_apply_affinity(column.ty(), target_register, self.program);
        }

        if self.program.flags.suppress_custom_type_decode() {
            self.finish_constant_span(constant_span);
            return Ok(());
        }

        if type_def.is_some_and(|td| td.encode().is_some()) {
            if let Some(default_expr) = column.default.as_ref() {
                // For custom type columns with ENCODE and a DEFAULT,
                // we suppressed the Column default so short records
                // (ALTER TABLE ADD COLUMN) return NULL.  Use
                // ColumnHasField to detect short records and compute
                // ENCODE(DEFAULT) at runtime via bytecode.
                let read_cursor = read_cursor.expect("cursor should be opened");
                let done_label = self.program.allocate_label();
                // Jump past the default block if the record
                // actually has this column (not a short record).
                self.program
                    .emit_column_has_field(read_cursor, column_idx, done_label);
                self.act(move |translator| {
                    let maybe_encode = translator
                        .resolver
                        .schema()
                        .get_type_def(&column.ty_str, table_is_strict)
                        .and_then(|type_def| {
                            type_def.encode().map(|expr| (type_def.as_ref(), expr))
                        });

                    translator.act(move |translator| {
                        translator.program.preassign_label_to_next_insn(done_label);
                        translator.push_finish_constant_span(constant_span);
                        translator.schedule_user_facing_column_value(
                            target_register,
                            target_register,
                            column,
                            table_is_strict,
                        )?;
                        Ok(())
                    });

                    if let Some((type_def, encode_expr)) = maybe_encode {
                        translator.schedule_type_expr(
                            encode_expr,
                            target_register,
                            target_register,
                            column,
                            type_def,
                        )?;
                    }
                    Ok(())
                });
                self.push_translate_no_constant_opt(
                    Some(referenced_tables),
                    default_expr,
                    target_register,
                );
                return Ok(());
            }
        }

        self.push_finish_constant_span(constant_span);
        // Decode custom type columns (skipped when building ORDER BY sort keys
        // for types without a `<` operator, so the sorter sorts on encoded values)
        self.schedule_user_facing_column_value(
            target_register,
            target_register,
            column,
            table_is_strict,
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn visit_binary_expr(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        e1: &'a ast::Expr,
        op: &'a ast::Operator,
        e2: &'a ast::Expr,
        target_register: usize,
        constant_span: Option<usize>,
    ) -> Result<()> {
        if let Some((is_not, is_true_literal)) = match (op, e2) {
            (ast::Operator::Is, ast::Expr::Literal(ast::Literal::True)) => Some((false, true)),
            (ast::Operator::Is, ast::Expr::Literal(ast::Literal::False)) => Some((false, false)),
            (ast::Operator::IsNot, ast::Expr::Literal(ast::Literal::True)) => Some((true, true)),
            (ast::Operator::IsNot, ast::Expr::Literal(ast::Literal::False)) => Some((true, false)),
            _ => None,
        } {
            // Handle IS TRUE/IS FALSE/IS NOT TRUE/IS NOT FALSE specially.
            // These use truth semantics, where only non-zero numbers are truthy,
            // rather than equality semantics.
            let reg = self.program.alloc_register();
            self.act(move |translator| {
                // For NULL: IS variants return 0, IS NOT variants return 1
                // For non-NULL: IS TRUE/IS NOT FALSE return truthy, IS FALSE/IS NOT TRUE return !truthy
                let null_value = is_not;
                let invert = is_not == is_true_literal;
                translator.program.emit_insn(Insn::IsTrue {
                    reg,
                    dest: target_register,
                    null_value,
                    invert,
                });
                translator.finish_constant_span(constant_span);
                Ok(())
            });
            self.visit(referenced_tables, e1, reg);
            return Ok(());
        }

        // Check if either operand has a custom type with a matching operator.
        if let Some(resolved) =
            find_custom_type_operator(e1, e2, op, referenced_tables, self.resolver)
        {
            self.schedule_custom_type_operator(
                referenced_tables,
                e1,
                e2,
                resolved,
                target_register,
                constant_span,
            )?;
            return Ok(());
        }

        self.schedule_binary_expr_shared(
            referenced_tables,
            e1,
            e2,
            *op,
            target_register,
            BinaryEmitMode::Value,
            constant_span,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn schedule_binary_expr_shared(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        e1: &'a ast::Expr,
        e2: &'a ast::Expr,
        op: ast::Operator,
        target_register: usize,
        emit_mode: BinaryEmitMode,
        constant_span: Option<usize>,
    ) -> Result<()> {
        let lhs_arity = expr_vector_size(e1)?;
        let rhs_arity = expr_vector_size(e2)?;
        if lhs_arity != rhs_arity {
            crate::bail_parse_error!(
                "all arguments to binary operator {op} must return the same number of values. Got: ({lhs_arity}) {op} ({rhs_arity})"
            );
        }

        if lhs_arity == 1 {
            return self.schedule_binary_expr_scalar(
                referenced_tables,
                e1,
                e2,
                op,
                target_register,
                emit_mode,
                constant_span,
            );
        }

        if !supports_row_value_binary_comparison(&op) {
            crate::bail_parse_error!("row value misused");
        }

        let lhs_reg = self.program.alloc_registers(lhs_arity);
        let rhs_reg = self.program.alloc_registers(lhs_arity);
        self.act(move |translator| {
            emit_binary_expr_row_valued(
                translator.program,
                &op,
                lhs_reg,
                rhs_reg,
                lhs_arity,
                target_register,
                e1,
                e2,
                referenced_tables,
                Some(translator.resolver),
            )?;
            if let BinaryEmitMode::Condition(metadata) = emit_mode {
                emit_cond_jump(translator.program, metadata, target_register);
            }
            translator.finish_constant_span(constant_span);
            Ok(())
        });
        self.visit(referenced_tables, e2, rhs_reg);
        self.visit(referenced_tables, e1, lhs_reg);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn schedule_binary_expr_scalar(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        e1: &'a ast::Expr,
        e2: &'a ast::Expr,
        op: ast::Operator,
        target_register: usize,
        emit_mode: BinaryEmitMode,
        constant_span: Option<usize>,
    ) -> Result<()> {
        if exprs_are_equivalent(e1, e2) {
            // Check if both sides of the expression are equivalent and reuse the
            // same register if so.
            let shared_reg = self.program.alloc_register();
            self.act(move |translator| {
                emit_binary_with_mode(
                    translator.program,
                    emit_mode,
                    &op,
                    shared_reg,
                    shared_reg,
                    target_register,
                    e1,
                    e2,
                    referenced_tables,
                    translator.resolver,
                )?;
                if op.is_comparison() {
                    translator.program.reset_collation();
                }
                translator.finish_constant_span(constant_span);
                Ok(())
            });
            self.visit(referenced_tables, e1, shared_reg);
            return Ok(());
        }

        let e1_reg = self.program.alloc_registers(2);
        let e2_reg = e1_reg + 1;
        self.act(move |translator| {
            let left_collation_ctx = translator.program.curr_collation_ctx();
            translator.program.reset_collation();
            translator.act(move |translator| {
                let right_collation_ctx = translator.program.curr_collation_ctx();
                translator.program.reset_collation();
                /*
                 * The rules for determining which collating function to use for a binary comparison
                 * operator (=, <, >, <=, >=, !=, IS, and IS NOT) are as follows:
                 *
                 * 1. If either operand has an explicit collating function assignment using the postfix COLLATE operator,
                 * then the explicit collating function is used for comparison,
                 * with precedence to the collating function of the left operand.
                 *
                 * 2. If either operand is a column, then the collating function of that column is used
                 * with precedence to the left operand. For the purposes of the previous sentence,
                 * a column name preceded by one or more unary "+" operators and/or CAST operators is still considered a column name.
                 *
                 * 3. Otherwise, the BINARY collating function is used for comparison.
                 */
                let collation_ctx = match (left_collation_ctx, right_collation_ctx) {
                    (Some((c_left, true)), _) => Some((c_left, true)),
                    (_, Some((c_right, true))) => Some((c_right, true)),
                    (Some((c_left, from_collate_left)), None) => Some((c_left, from_collate_left)),
                    (None, Some((c_right, from_collate_right))) => {
                        Some((c_right, from_collate_right))
                    }
                    (Some((c_left, from_collate_left)), Some((_, false))) => {
                        Some((c_left, from_collate_left))
                    }
                    _ => None,
                };
                translator.program.set_collation(collation_ctx);
                emit_binary_with_mode(
                    translator.program,
                    emit_mode,
                    &op,
                    e1_reg,
                    e2_reg,
                    target_register,
                    e1,
                    e2,
                    referenced_tables,
                    translator.resolver,
                )?;
                // Only reset collation for comparison operators, which consume it.
                // Non-comparison operators (Concat, Add, etc.) must propagate the
                // collation to the parent expression so that e.g.
                //   (name COLLATE NOCASE || '') <> 'admin'
                // correctly applies NOCASE to the Ne comparison.
                if op.is_comparison() {
                    translator.program.reset_collation();
                }
                translator.finish_constant_span(constant_span);
                Ok(())
            });
            translator.visit(referenced_tables, e2, e2_reg);
            Ok(())
        });
        self.visit(referenced_tables, e1, e1_reg);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn schedule_between_expr(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        lhs: &'a ast::Expr,
        not: bool,
        start: &'a ast::Expr,
        end: &'a ast::Expr,
        target_register: usize,
        constant_span: Option<usize>,
    ) -> Result<()> {
        let lhs_arity = expr_vector_size(lhs)?;
        let start_arity = expr_vector_size(start)?;
        let end_arity = expr_vector_size(end)?;
        if lhs_arity != start_arity || lhs_arity != end_arity {
            crate::bail_parse_error!(
                "all arguments to BETWEEN must return the same number of values. Got: ({lhs_arity}) BETWEEN ({start_arity}) AND ({end_arity})"
            );
        }

        let lhs_reg = self.program.alloc_registers(lhs_arity);
        let lhs_collation = match referenced_tables {
            Some(referenced_tables) => get_expr_collation_ctx(lhs, referenced_tables)?,
            None => None,
        };

        let (lower_op, upper_op, combine_op) = if not {
            (
                ast::Operator::Less,
                ast::Operator::Greater,
                ast::Operator::Or,
            )
        } else {
            (
                ast::Operator::GreaterEquals,
                ast::Operator::LessEquals,
                ast::Operator::And,
            )
        };

        self.act(move |translator| {
            translator.push_local_expr_cache(lhs, lhs_reg, lhs_arity, false, lhs_collation);
            let lower_reg = translator.program.alloc_register();
            translator.act(move |translator| {
                let upper_reg = translator.program.alloc_register();
                translator.act(move |translator| {
                    translator.pop_local_expr_cache();
                    translator.program.emit_insn(match combine_op {
                        ast::Operator::And => Insn::And {
                            lhs: lower_reg,
                            rhs: upper_reg,
                            dest: target_register,
                        },
                        ast::Operator::Or => Insn::Or {
                            lhs: lower_reg,
                            rhs: upper_reg,
                            dest: target_register,
                        },
                        _ => unreachable!("BETWEEN combine operator must be AND/OR"),
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                translator.schedule_binary_expr_shared(
                    referenced_tables,
                    lhs,
                    end,
                    upper_op,
                    upper_reg,
                    BinaryEmitMode::Value,
                    None,
                )
            });
            translator.schedule_binary_expr_shared(
                referenced_tables,
                lhs,
                start,
                lower_op,
                lower_reg,
                BinaryEmitMode::Value,
                None,
            )
        });
        self.visit(referenced_tables, lhs, lhs_reg);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn schedule_case_expr(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        base: Option<&'a ast::Expr>,
        when_then_pairs: &'a [(Box<ast::Expr>, Box<ast::Expr>)],
        else_expr: Option<&'a ast::Expr>,
        target_register: usize,
        constant_span: Option<usize>,
    ) {
        // There's two forms of CASE, one which checks a base expression for equality
        // against the WHEN values, and returns the corresponding THEN value if it matches:
        //   CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END
        // And one which evaluates a series of boolean predicates:
        //   CASE WHEN is_good THEN 'good' WHEN is_bad THEN 'bad' ELSE 'okay' END
        // This just changes which sort of branching instruction to issue, after we
        // generate the expression if needed.
        let return_label = self.program.allocate_label();
        let first_case_label = self.program.allocate_label();
        // Only allocate a reg to hold the base expression if one was provided.
        // And base_reg then becomes the flag we check to see which sort of
        // case statement we're processing.
        let base_reg = base.map(|_| self.program.alloc_register());
        let expr_reg = self.program.alloc_register();

        self.act(move |translator| {
            translator.schedule_case_pair(
                referenced_tables,
                when_then_pairs,
                else_expr,
                target_register,
                expr_reg,
                base_reg,
                return_label,
                first_case_label,
                0,
                constant_span,
            );
            Ok(())
        });
        if let (Some(base_expr), Some(base_reg)) = (base, base_reg) {
            self.visit(referenced_tables, base_expr, base_reg);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn schedule_case_pair(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        when_then_pairs: &'a [(Box<ast::Expr>, Box<ast::Expr>)],
        else_expr: Option<&'a ast::Expr>,
        target_register: usize,
        expr_reg: usize,
        base_reg: Option<usize>,
        return_label: BranchOffset,
        next_case_label: BranchOffset,
        pair_index: usize,
        constant_span: Option<usize>,
    ) {
        let Some((when_expr, then_expr)) = when_then_pairs.get(pair_index) else {
            self.act(move |translator| {
                match else_expr {
                    Some(expr) => {
                        translator.act(move |translator| {
                            translator
                                .program
                                .preassign_label_to_next_insn(return_label);
                            translator.finish_constant_span(constant_span);
                            Ok(())
                        });
                        translator.push_translate_no_constant_opt(
                            referenced_tables,
                            expr,
                            target_register,
                        );
                    }
                    None => {
                        translator.program.emit_insn(Insn::Null {
                            dest: target_register,
                            dest_end: None,
                        });
                        translator
                            .program
                            .preassign_label_to_next_insn(return_label);
                        translator.finish_constant_span(constant_span);
                    }
                }
                Ok(())
            });
            return;
        };

        self.act(move |translator| {
            match base_reg {
                Some(base_reg) => translator.program.emit_insn(Insn::Ne {
                    lhs: base_reg,
                    rhs: expr_reg,
                    target_pc: next_case_label,
                    flags: CmpInsFlags::default().jump_if_null(),
                    collation: translator.program.curr_collation(),
                }),
                None => translator.program.emit_insn(Insn::IfNot {
                    reg: expr_reg,
                    target_pc: next_case_label,
                    jump_if_null: true,
                }),
            }

            translator.act(move |translator| {
                translator.program.emit_insn(Insn::Goto {
                    target_pc: return_label,
                });
                translator
                    .program
                    .preassign_label_to_next_insn(next_case_label);
                // This becomes either the next WHEN, or in the last WHEN/THEN, we're
                // assured to have at least one instruction corresponding to the ELSE immediately follow.
                let next_label = translator.program.allocate_label();
                translator.schedule_case_pair(
                    referenced_tables,
                    when_then_pairs,
                    else_expr,
                    target_register,
                    expr_reg,
                    base_reg,
                    return_label,
                    next_label,
                    pair_index + 1,
                    constant_span,
                );
                Ok(())
            });
            translator.push_translate_no_constant_opt(
                referenced_tables,
                then_expr,
                target_register,
            );
            Ok(())
        });
        self.push_translate_no_constant_opt(referenced_tables, when_expr, expr_reg);
    }

    #[allow(clippy::too_many_arguments)]
    fn schedule_in_list_expr(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        lhs: &'a ast::Expr,
        rhs: &'a [Box<ast::Expr>],
        not: bool,
        target_register: usize,
        constant_span: Option<usize>,
    ) -> Result<()> {
        // Following SQLite's approach: use the same core logic as conditional InList,
        // but wrap it with appropriate expression context handling
        let dest_if_false = self.program.allocate_label();
        let dest_if_null = self.program.allocate_label();
        let dest_if_true = self.program.allocate_label();
        // Ideally we wouldn't need a tmp register, but currently if an IN expression
        // is used inside an aggregator the target_register is cleared on every iteration,
        // losing the state of the aggregator.
        let tmp = self.program.alloc_register();
        self.program.emit_no_constant_insn(Insn::Null {
            dest: tmp,
            dest_end: None,
        });

        self.act(move |translator| {
            // condition true: set result to 1.
            translator.program.emit_insn(Insn::Integer {
                value: 1,
                dest: tmp,
            });
            translator
                .program
                .preassign_label_to_next_insn(dest_if_false);
            // False path: set result to 0.
            // Force integer conversion with AddImm 0.
            translator.program.emit_insn(Insn::AddImm {
                register: tmp,
                value: 0,
            });
            if not {
                translator.program.emit_insn(Insn::Not {
                    reg: tmp,
                    dest: tmp,
                });
            }
            translator
                .program
                .preassign_label_to_next_insn(dest_if_null);
            translator.program.emit_insn(Insn::Copy {
                src_reg: tmp,
                dst_reg: target_register,
                extra_amount: 0,
            });
            translator.finish_constant_span(constant_span);
            Ok(())
        });
        self.schedule_in_list_condition(
            referenced_tables,
            lhs,
            rhs,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: dest_if_true,
                jump_target_when_false: dest_if_false,
                jump_target_when_null: dest_if_null,
            },
        )
    }

    fn schedule_in_list_condition(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        lhs: &'a ast::Expr,
        rhs: &'a [Box<ast::Expr>],
        condition_metadata: ConditionMetadata,
    ) -> Result<()> {
        let lhs_arity = expr_vector_size(lhs)?;
        let lhs_reg = self.program.alloc_registers(lhs_arity);
        let label_ok = self.program.allocate_label();
        // Compute the affinity for the IN comparison based on the LHS expression
        // This follows SQLite's exprINAffinity() approach
        let affinity = in_expr_affinity(lhs, referenced_tables, Some(self.resolver));
        let cmp_flags = CmpInsFlags::default().with_affinity(affinity);
        let check_null_reg = if condition_metadata.jump_target_when_false
            != condition_metadata.jump_target_when_null
        {
            Some(self.program.alloc_register())
        } else {
            None
        };

        self.act(move |translator| {
            if let Some(check_null_reg) = check_null_reg {
                translator.program.emit_insn(Insn::BitAnd {
                    lhs: lhs_reg,
                    rhs: lhs_reg,
                    dest: check_null_reg,
                });
            }
            translator.schedule_in_list_rhs(
                referenced_tables,
                lhs,
                rhs,
                lhs_arity,
                lhs_reg,
                check_null_reg,
                label_ok,
                cmp_flags,
                condition_metadata,
                0,
            )?;
            Ok(())
        });
        self.visit(referenced_tables, lhs, lhs_reg);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn schedule_in_list_rhs(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        lhs: &'a ast::Expr,
        rhs: &'a [Box<ast::Expr>],
        lhs_arity: usize,
        lhs_reg: usize,
        check_null_reg: Option<usize>,
        label_ok: BranchOffset,
        cmp_flags: CmpInsFlags,
        condition_metadata: ConditionMetadata,
        rhs_index: usize,
    ) -> Result<()> {
        let Some(expr) = rhs.get(rhs_index) else {
            self.act(move |translator| {
                if let Some(check_null_reg) = check_null_reg {
                    translator.program.emit_insn(Insn::IsNull {
                        reg: check_null_reg,
                        target_pc: condition_metadata.jump_target_when_null,
                    });
                    translator.program.emit_insn(Insn::Goto {
                        target_pc: condition_metadata.jump_target_when_false,
                    });
                }
                // we don't know exactly what instruction will came next and it's important to chain label to the execution flow rather then exact next instruction
                // for example, next instruction can be register assignment, which can be moved by optimized to the constant section
                // in this case, label_ok must be changed accordingly and be re-binded to another instruction followed the current translation unit after constants reording
                translator.program.preassign_label_to_next_insn(label_ok);
                if condition_metadata.jump_if_condition_is_true {
                    translator.program.emit_insn(Insn::Goto {
                        target_pc: condition_metadata.jump_target_when_true,
                    });
                }
                Ok(())
            });
            return Ok(());
        };

        let rhs_reg = self.program.alloc_registers(lhs_arity);
        self.act(move |translator| {
            let last_condition = rhs_index == rhs.len() - 1;
            if let Some(check_null_reg) = check_null_reg {
                if expr.can_be_null() {
                    translator.program.emit_insn(Insn::BitAnd {
                        lhs: check_null_reg,
                        rhs: rhs_reg,
                        dest: check_null_reg,
                    });
                }
            }

            if lhs_arity == 1 {
                if !last_condition
                    || condition_metadata.jump_target_when_false
                        != condition_metadata.jump_target_when_null
                {
                    if lhs_reg != rhs_reg {
                        translator.program.emit_insn(Insn::Eq {
                            lhs: lhs_reg,
                            rhs: rhs_reg,
                            target_pc: label_ok,
                            flags: cmp_flags,
                            collation: translator.program.curr_collation(),
                        });
                    } else {
                        translator.program.emit_insn(Insn::NotNull {
                            reg: lhs_reg,
                            target_pc: label_ok,
                        });
                    }
                } else if lhs_reg != rhs_reg {
                    translator.program.emit_insn(Insn::Ne {
                        lhs: lhs_reg,
                        rhs: rhs_reg,
                        target_pc: condition_metadata.jump_target_when_false,
                        flags: cmp_flags.jump_if_null(),
                        collation: translator.program.curr_collation(),
                    });
                } else {
                    translator.program.emit_insn(Insn::IsNull {
                        reg: lhs_reg,
                        target_pc: condition_metadata.jump_target_when_false,
                    });
                }
            } else if !last_condition
                || condition_metadata.jump_target_when_false
                    != condition_metadata.jump_target_when_null
            {
                let skip_label = translator.program.allocate_label();
                for j in 0..lhs_arity {
                    let (aff, collation) = row_component_affinity_collation(
                        lhs,
                        expr,
                        j,
                        referenced_tables,
                        Some(translator.resolver),
                    )?;
                    let flags = CmpInsFlags::default().with_affinity(aff);
                    if j < lhs_arity - 1 {
                        translator.program.emit_insn(Insn::Ne {
                            lhs: lhs_reg + j,
                            rhs: rhs_reg + j,
                            target_pc: skip_label,
                            flags,
                            collation,
                        });
                    } else {
                        translator.program.emit_insn(Insn::Eq {
                            lhs: lhs_reg + j,
                            rhs: rhs_reg + j,
                            target_pc: label_ok,
                            flags,
                            collation,
                        });
                    }
                }
                translator.program.preassign_label_to_next_insn(skip_label);
            } else {
                for j in 0..lhs_arity {
                    let (aff, collation) = row_component_affinity_collation(
                        lhs,
                        expr,
                        j,
                        referenced_tables,
                        Some(translator.resolver),
                    )?;
                    let flags = CmpInsFlags::default().with_affinity(aff).jump_if_null();
                    translator.program.emit_insn(Insn::Ne {
                        lhs: lhs_reg + j,
                        rhs: rhs_reg + j,
                        target_pc: condition_metadata.jump_target_when_false,
                        flags,
                        collation,
                    });
                }
            }

            translator.schedule_in_list_rhs(
                referenced_tables,
                lhs,
                rhs,
                lhs_arity,
                lhs_reg,
                check_null_reg,
                label_ok,
                cmp_flags,
                condition_metadata,
                rhs_index + 1,
            )?;
            Ok(())
        });
        self.visit(referenced_tables, expr, rhs_reg);
        Ok(())
    }

    fn schedule_field_access(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        base: &'a ast::Expr,
        field: &'a ast::Name,
        resolved: Option<&'a ast::FieldAccessResolution>,
        target_register: usize,
        constant_span: Option<usize>,
    ) {
        let base_reg = self.program.alloc_register();
        self.act(move |translator| {
            if let Some(resolution) = resolved {
                match resolution {
                    ast::FieldAccessResolution::StructField { field_index } => {
                        translator.program.emit_insn(Insn::StructField {
                            src_reg: base_reg,
                            field_index: *field_index,
                            dest: target_register,
                        });
                    }
                    ast::FieldAccessResolution::UnionVariant { tag_index } => {
                        translator.program.emit_insn(Insn::UnionExtract {
                            src_reg: base_reg,
                            expected_tag: *tag_index,
                            dest: target_register,
                        });
                    }
                }
                translator.finish_constant_span(constant_span);
                return Ok(());
            }

            // Slow path: recursively resolve the base expression's output type,
            // then look up the field/variant in that type.
            let td = resolve_expr_output_type(base, referenced_tables, translator.resolver)?;
            let field_name = normalize_ident(field.as_str());
            if let Some((idx, _)) = td.find_struct_field(&field_name) {
                translator.program.emit_insn(Insn::StructField {
                    src_reg: base_reg,
                    field_index: idx,
                    dest: target_register,
                });
            } else if let Some(tag_index) = td.resolve_union_tag_index(&field_name) {
                translator.program.emit_insn(Insn::UnionExtract {
                    src_reg: base_reg,
                    expected_tag: tag_index,
                    dest: target_register,
                });
            } else if td.is_struct() {
                crate::bail_parse_error!(
                    "no such field '{}' in struct type '{}'",
                    field_name,
                    td.name
                );
            } else if td.is_union() {
                crate::bail_parse_error!(
                    "no such variant '{}' in union type '{}'",
                    field_name,
                    td.name
                );
            } else {
                crate::bail_parse_error!("type '{}' is not a struct or union type", td.name);
            }
            translator.finish_constant_span(constant_span);
            Ok(())
        });
        self.visit(referenced_tables, base, base_reg);
    }

    fn schedule_raise_expr(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        resolve_type: ResolveType,
        msg_expr: Option<&'a ast::Expr>,
        target_register: usize,
        constant_span: Option<usize>,
    ) -> Result<()> {
        let in_trigger = self.program.trigger.is_some();
        match resolve_type {
            ResolveType::Ignore => {
                if !in_trigger {
                    crate::bail_parse_error!("RAISE() may only be used within a trigger-program");
                }
                self.program.emit_insn(Insn::Halt {
                    err_code: 0,
                    description: String::new(),
                    on_error: Some(ResolveType::Ignore),
                    description_reg: None,
                });
                self.finish_constant_span(constant_span);
            }
            ResolveType::Fail | ResolveType::Abort | ResolveType::Rollback => {
                if !in_trigger && resolve_type != ResolveType::Abort {
                    crate::bail_parse_error!("RAISE() may only be used within a trigger-program");
                }
                let err_code = if in_trigger {
                    SQLITE_CONSTRAINT_TRIGGER
                } else {
                    SQLITE_ERROR
                };
                match msg_expr {
                    Some(ast::Expr::Literal(ast::Literal::String(s))) => {
                        self.program.emit_insn(Insn::Halt {
                            err_code,
                            description: sanitize_string(s),
                            on_error: Some(resolve_type),
                            description_reg: None,
                        });
                        self.finish_constant_span(constant_span);
                    }
                    Some(e) => {
                        let reg = self.program.alloc_register();
                        self.act(move |translator| {
                            translator.program.emit_insn(Insn::Halt {
                                err_code,
                                description: String::new(),
                                on_error: Some(resolve_type),
                                description_reg: Some(reg),
                            });
                            translator.finish_constant_span(constant_span);
                            Ok(())
                        });
                        self.visit(referenced_tables, e, reg);
                    }
                    None => {
                        crate::bail_parse_error!("RAISE requires an error message");
                    }
                }
            }
            ResolveType::Replace => {
                crate::bail_parse_error!("REPLACE is not valid for RAISE");
            }
        }
        let _ = target_register;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn schedule_custom_type_operator(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        e1: &'a ast::Expr,
        e2: &'a ast::Expr,
        resolved: ResolvedOperator<'a>,
        target_register: usize,
        constant_span: Option<usize>,
    ) -> Result<()> {
        let func = self
            .resolver
            .resolve_function(&resolved.func_name, 2)?
            .ok_or_else(|| {
                LimboError::InternalError(format!("function not found: {}", resolved.func_name))
            })?;
        let (first, second) = if resolved.swap_args {
            (e2, e1)
        } else {
            (e1, e2)
        };

        match resolved.encode_info {
            Some(encode_info) => {
                if encode_info.type_def.encode().is_some() {
                    let tmp1 = self.program.alloc_register();
                    let tmp2 = self.program.alloc_register();
                    let swap_args = resolved.swap_args;
                    let negate = resolved.negate;
                    self.act(move |translator| {
                        // When encoding a literal operand, we must use separate registers for the
                        // function call arguments. translate_expr may place literals in preamble
                        // registers (constant optimization), and encoding in-place would clobber
                        // that register — breaking subsequent loop iterations.
                        let encode_expr = encode_info
                            .type_def
                            .encode()
                            .expect("encode expression checked before scheduling");
                        // Determine which tmp holds the literal and which holds the column.
                        let (lit_tmp, col_tmp) = match encode_info.which {
                            EncodeArg::First if swap_args => (tmp2, tmp1),
                            EncodeArg::First => (tmp1, tmp2),
                            EncodeArg::Second if swap_args => (tmp1, tmp2),
                            EncodeArg::Second => (tmp2, tmp1),
                        };

                        // Allocate fresh contiguous registers for the function call.
                        let func_args = translator.program.alloc_registers(2);
                        // The literal goes in the same position it occupied in arg layout.
                        let (lit_dst, col_dst) = match encode_info.which {
                            EncodeArg::First if swap_args => (func_args + 1, func_args),
                            EncodeArg::First => (func_args, func_args + 1),
                            EncodeArg::Second if swap_args => (func_args, func_args + 1),
                            EncodeArg::Second => (func_args + 1, func_args),
                        };

                        // Copy column value as-is.
                        translator.program.emit_insn(Insn::Copy {
                            src_reg: col_tmp,
                            dst_reg: col_dst,
                            extra_amount: 0,
                        });
                        translator.act(move |translator| {
                            translator.emit_custom_type_operator_result(
                                func_args,
                                target_register,
                                func,
                                negate,
                                constant_span,
                            );
                            Ok(())
                        });
                        // Encode the literal into the fresh function arg slot.
                        translator.schedule_type_expr(
                            encode_expr,
                            lit_tmp,
                            lit_dst,
                            encode_info.column,
                            encode_info.type_def,
                        )?;
                        Ok(())
                    });
                    self.visit(referenced_tables, second, tmp2);
                    self.visit(referenced_tables, first, tmp1);
                } else {
                    let arg_reg = self.program.alloc_registers(2);
                    let negate = resolved.negate;
                    self.act(move |translator| {
                        translator.emit_custom_type_operator_result(
                            arg_reg,
                            target_register,
                            func,
                            negate,
                            constant_span,
                        );
                        Ok(())
                    });
                    self.visit(referenced_tables, second, arg_reg + 1);
                    self.visit(referenced_tables, first, arg_reg);
                }
            }
            None => {
                let arg_reg = self.program.alloc_registers(2);
                let negate = resolved.negate;
                self.act(move |translator| {
                    translator.emit_custom_type_operator_result(
                        arg_reg,
                        target_register,
                        func,
                        negate,
                        constant_span,
                    );
                    Ok(())
                });
                self.visit(referenced_tables, second, arg_reg + 1);
                self.visit(referenced_tables, first, arg_reg);
            }
        }
        Ok(())
    }

    fn emit_custom_type_operator_result(
        &mut self,
        func_start: usize,
        target_register: usize,
        func: Func,
        negate: bool,
        constant_span: Option<usize>,
    ) {
        let result_reg = self.program.alloc_register();
        self.program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: func_start,
            dest: result_reg,
            func: FuncCtx { func, arg_count: 2 },
        });
        if negate {
            self.program.emit_insn(Insn::Not {
                reg: result_reg,
                dest: result_reg,
            });
        }
        if result_reg != target_register {
            self.program.emit_insn(Insn::Copy {
                src_reg: result_reg,
                dst_reg: target_register,
                extra_amount: 0,
            });
        }
        self.finish_constant_span(constant_span);
    }

    /// Emits a whole insn for a function call.
    /// Assumes the number of parameters is valid for the given function.
    /// Returns the target register for the function.
    fn schedule_function_insn(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        args: &'a [Box<ast::Expr>],
        start_reg: usize,
        target_register: usize,
        func_ctx: FuncCtx,
        constant_span: Option<usize>,
    ) {
        self.act(move |translator| {
            translator.program.emit_insn(Insn::Function {
                constant_mask: 0,
                start_reg,
                dest: target_register,
                func: func_ctx,
            });
            translator.finish_constant_span(constant_span);
            Ok(())
        });
        self.push_translate_args(referenced_tables, args, start_reg);
    }

    fn schedule_make_array_insn(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        args: &'a [Box<ast::Expr>],
        target_register: usize,
        constant_span: Option<usize>,
    ) {
        // Translate a function that is desugared into a dedicated variadic-argument instruction.
        // Evaluates all args into consecutive registers, then emits the instruction.
        // The instruction must have fields `start_reg`, `count`, and `dest`.
        //
        // Used by: Array (MakeArray), struct_pack (also MakeArray).
        let start_reg = self.program.alloc_registers(args.len());
        self.act(move |translator| {
            translator.program.emit_insn(Insn::MakeArray {
                start_reg,
                count: args.len(),
                dest: target_register,
            });
            translator.finish_constant_span(constant_span);
            Ok(())
        });
        self.push_translate_args(referenced_tables, args, start_reg);
    }

    fn emit_function_now(
        &mut self,
        start_reg: usize,
        target_register: usize,
        func_ctx: FuncCtx,
        constant_span: Option<usize>,
    ) {
        self.program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg,
            dest: target_register,
            func: func_ctx,
        });
        self.finish_constant_span(constant_span);
    }

    fn schedule_function_call_star(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        name: &'a ast::Name,
        filter_over: &'a ast::FunctionTail,
        target_register: usize,
        constant_span: Option<usize>,
    ) -> Result<()> {
        // Handle func(*) syntax as a function call with 0 arguments
        // This is equivalent to func() for functions that accept 0 arguments
        let Some(func) = self.resolver.resolve_function(name.as_str(), 0)? else {
            crate::bail_parse_error!("no such function: {}", name.as_str());
        };

        match &func {
            Func::Agg(_) => {
                crate::bail_parse_error!(
                    "misuse of {} function {}(*)",
                    if filter_over.over_clause.is_some() {
                        "window"
                    } else {
                        "aggregate"
                    },
                    name.as_str()
                )
            }
            Func::Window(_) => {
                crate::bail_parse_error!("misuse of window function {}()", name.as_str())
            }
            _ if func.needs_star_expansion() => {
                // For functions that need star expansion (json_object, jsonb_object),
                // expand the * to all columns from the referenced tables as key-value pairs.
                let tables = referenced_tables.ok_or_else(|| {
                    LimboError::ParseError(format!("{}(*) requires a FROM clause", name.as_str()))
                })?;
                // Verify there's at least one table to expand.
                if tables.joined_tables().is_empty() {
                    return Err(LimboError::ParseError(format!(
                        "{}(*) requires a FROM clause",
                        name.as_str()
                    )));
                }

                let mut expanded_columns = Vec::new();
                // Build arguments: alternating column_name (as string literal), column_value
                // (as column reference).
                for table in tables.joined_tables() {
                    for (col_idx, col) in table.columns().iter().enumerate() {
                        // Skip hidden columns, such as rowid in some cases.
                        if col.hidden() {
                            continue;
                        }
                        let col_name = col
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("column{}", col_idx + 1));
                        expanded_columns.push((
                            col_name,
                            table.internal_id,
                            col_idx,
                            col.is_rowid_alias(),
                        ));
                    }
                }

                let args_count = expanded_columns.len() * 2;
                let Some(expanded_func) =
                    self.resolver.resolve_function(name.as_str(), args_count)?
                else {
                    crate::bail_parse_error!("no such function: {}", name.as_str());
                };
                let func_ctx = FuncCtx {
                    func: expanded_func,
                    arg_count: args_count,
                };
                let start_reg = self.program.alloc_registers(args_count);
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::Function {
                        constant_mask: 0,
                        start_reg,
                        dest: target_register,
                        func: func_ctx,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });

                for (i, (col_name, table_id, column, is_rowid_alias)) in
                    expanded_columns.into_iter().enumerate().rev()
                {
                    let key_reg = start_reg + (i * 2);
                    let value_reg = key_reg + 1;
                    // Add column reference using Expr::Column.
                    self.act(move |translator| {
                        translator.schedule_column_expr(
                            referenced_tables,
                            table_id,
                            column,
                            is_rowid_alias,
                            value_reg,
                            None,
                        )
                    });
                    self.act(move |translator| {
                        // Add column name as a string literal
                        // Note: ast::Literal::String values must be wrapped in single quotes
                        // because sanitize_string() strips the first and last character
                        let constant_span = if !translator.program.constant_span_is_open() {
                            Some(translator.program.constant_span_start())
                        } else {
                            None
                        };
                        translator.program.emit_insn(Insn::String8 {
                            value: col_name,
                            dest: key_reg,
                        });
                        translator.finish_constant_span(constant_span);
                        Ok(())
                    });
                }
                Ok(())
            }
            _ => {
                // For supported functions, delegate to the existing FunctionCall logic
                // by creating a synthetic FunctionCall with empty args
                let empty_args: &'a [Box<ast::Expr>] = &[];
                self.schedule_function_call(
                    referenced_tables,
                    name,
                    empty_args,
                    filter_over,
                    target_register,
                    constant_span,
                )
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn schedule_function_call(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        name: &'a ast::Name,
        args: &'a [Box<ast::Expr>],
        filter_over: &'a ast::FunctionTail,
        target_register: usize,
        constant_span: Option<usize>,
    ) -> Result<()> {
        let args_count = args.len();
        let Some(func) = self.resolver.resolve_function(name.as_str(), args_count)? else {
            crate::bail_parse_error!("no such function: {}", name.as_str());
        };

        let func_ctx = FuncCtx {
            func,
            arg_count: args_count,
        };

        match func_ctx.func.clone() {
            Func::Agg(_) => {
                crate::bail_parse_error!(
                    "misuse of {} function {}()",
                    if filter_over.over_clause.is_some() {
                        "window"
                    } else {
                        "aggregate"
                    },
                    name.as_str()
                )
            }
            Func::Window(_) => {
                crate::bail_parse_error!("misuse of window function {}()", name.as_str())
            }
            Func::External(_) => {
                let regs = self.program.alloc_registers(args_count);
                let arg_registers: Vec<usize> = (regs..regs + args_count).collect();
                self.act(move |translator| {
                    emit_function_call(
                        translator.program,
                        func_ctx,
                        &arg_registers,
                        target_register,
                    )?;
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.push_translate_args(referenced_tables, args, regs);
                Ok(())
            }
            #[cfg(feature = "json")]
            Func::Json(j) => {
                match j {
                    JsonFunc::Json | JsonFunc::Jsonb => {
                        let args = expect_arguments_exact!(args, 1, j);
                        let start_reg = self.program.alloc_registers(args.len());
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                    JsonFunc::JsonArray
                    | JsonFunc::JsonbArray
                    | JsonFunc::JsonExtract
                    | JsonFunc::JsonSet
                    | JsonFunc::JsonbSet
                    | JsonFunc::JsonbExtract
                    | JsonFunc::JsonReplace
                    | JsonFunc::JsonbReplace
                    | JsonFunc::JsonbRemove
                    | JsonFunc::JsonInsert
                    | JsonFunc::JsonbInsert => {
                        let start_reg = self.program.alloc_registers(args.len());
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                    JsonFunc::JsonArrowExtract | JsonFunc::JsonArrowShiftExtract => {
                        unreachable!(
                            "These two functions are only reachable via the -> and ->> operators"
                        )
                    }
                    JsonFunc::JsonArrayLength | JsonFunc::JsonType => {
                        let args = expect_arguments_max!(args, 2, j);
                        let start_reg = self.program.alloc_registers(args.len());
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                    JsonFunc::JsonErrorPosition => {
                        if args.len() != 1 {
                            crate::bail_parse_error!(
                                "{} function with not exactly 1 argument",
                                j.to_string()
                            );
                        }
                        let start_reg = self.program.alloc_register();
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                    JsonFunc::JsonObject | JsonFunc::JsonbObject => {
                        let args = expect_arguments_even!(args, j);
                        let start_reg = self.program.alloc_registers(args.len());
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                    JsonFunc::JsonValid => {
                        let args = expect_arguments_exact!(args, 1, j);
                        let start_reg = self.program.alloc_registers(args.len());
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                    JsonFunc::JsonPatch | JsonFunc::JsonbPatch => {
                        let args = expect_arguments_exact!(args, 2, j);
                        let start_reg = self.program.alloc_registers(args.len());
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                    JsonFunc::JsonRemove => {
                        let start_reg = self.program.alloc_registers(args.len().max(1));
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                    JsonFunc::JsonQuote => {
                        let args = expect_arguments_exact!(args, 1, j);
                        let start_reg = self.program.alloc_registers(args.len());
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                    JsonFunc::JsonPretty => {
                        let args = expect_arguments_max!(args, 2, j);
                        let start_reg = self.program.alloc_registers(args.len());
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                }
                Ok(())
            }
            Func::Vector(vector_func) => {
                match vector_func {
                    VectorFunc::Vector
                    | VectorFunc::Vector32
                    | VectorFunc::Vector32Sparse
                    | VectorFunc::Vector64
                    | VectorFunc::Vector8
                    | VectorFunc::Vector1Bit
                    | VectorFunc::VectorExtract => {
                        let args = expect_arguments_exact!(args, 1, vector_func);
                        let start_reg = self.program.alloc_register();
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                    VectorFunc::VectorDistanceCos
                    | VectorFunc::VectorDistanceL2
                    | VectorFunc::VectorDistanceJaccard
                    | VectorFunc::VectorDistanceDot
                    | VectorFunc::VectorConcat => {
                        let args = expect_arguments_exact!(args, 2, vector_func);
                        let start_reg = self.program.alloc_registers(2);
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                    VectorFunc::VectorSlice => {
                        let args = expect_arguments_exact!(args, 3, vector_func);
                        let start_reg = self.program.alloc_registers(3);
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                }
                Ok(())
            }
            Func::Scalar(srf) => self.schedule_scalar_function_call(
                referenced_tables,
                args,
                target_register,
                func_ctx,
                srf,
                constant_span,
            ),
            Func::Math(math_func) => {
                match math_func.arity() {
                    MathFuncArity::Nullary => {
                        if !args.is_empty() {
                            crate::bail_parse_error!("{} function with arguments", math_func);
                        }
                        self.emit_function_now(0, target_register, func_ctx, constant_span);
                    }
                    MathFuncArity::Unary => {
                        let args = expect_arguments_exact!(args, 1, math_func);
                        let start_reg = self.program.alloc_register();
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                    MathFuncArity::Binary => {
                        let args = expect_arguments_exact!(args, 2, math_func);
                        let start_reg = self.program.alloc_registers(2);
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                    MathFuncArity::UnaryOrBinary => {
                        let args = expect_arguments_max!(args, 2, math_func);
                        let start_reg = self.program.alloc_registers(args.len());
                        self.schedule_function_insn(
                            referenced_tables,
                            args,
                            start_reg,
                            target_register,
                            func_ctx,
                            constant_span,
                        );
                    }
                }
                Ok(())
            }
            #[cfg(all(feature = "fts", not(target_family = "wasm")))]
            Func::Fts(_) => {
                // FTS functions are handled via index method pattern matching.
                // If we reach here, no index matched, so translate as a regular function call.
                let start_reg = self.program.alloc_registers(args.len());
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
                Ok(())
            }
            Func::AlterTable(_) => unreachable!(),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn schedule_scalar_function_call(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        args: &'a [Box<ast::Expr>],
        target_register: usize,
        func_ctx: FuncCtx,
        srf: ScalarFunc,
        constant_span: Option<usize>,
    ) -> Result<()> {
        match srf {
            ScalarFunc::Cast => unreachable!("this is always ast::Expr::Cast"),
            // Arity and custom-types checks are done at bind time
            // in validate_custom_type_function_call.
            ScalarFunc::Array | ScalarFunc::StructPack => {
                self.schedule_make_array_insn(
                    referenced_tables,
                    args,
                    target_register,
                    constant_span,
                );
            }
            ScalarFunc::ArrayElement => {
                // Translate a function that is desugared into a dedicated fixed-argument instruction.
                // Evaluates each arg into its own register, then emits the instruction using a
                // caller-provided expression that can reference the allocated registers by index.
                //
                // `$reg_names` is a comma-separated list of identifiers bound to allocated
                // registers for the corresponding arg indices.
                //
                // Used by: ArrayElement, ArraySetElement, UnionTag, UnionExtract, UnionValue.
                let args = expect_arguments_exact!(args, 2, srf);
                let array_reg = self.program.alloc_register();
                let index_reg = self.program.alloc_register();
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::ArrayElement {
                        array_reg,
                        index_reg,
                        dest: target_register,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, &args[1], index_reg);
                self.visit(referenced_tables, &args[0], array_reg);
            }
            ScalarFunc::ArraySetElement => {
                let args = expect_arguments_exact!(args, 3, srf);
                let array_reg = self.program.alloc_register();
                let index_reg = self.program.alloc_register();
                let value_reg = self.program.alloc_register();
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::ArraySetElement {
                        array_reg,
                        index_reg,
                        value_reg,
                        dest: target_register,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, &args[2], value_reg);
                self.visit(referenced_tables, &args[1], index_reg);
                self.visit(referenced_tables, &args[0], array_reg);
            }
            ScalarFunc::Changes => {
                if !args.is_empty() {
                    crate::bail_parse_error!("{} function with more than 0 arguments", srf);
                }
                let start_reg = self.program.alloc_register();
                self.emit_function_now(start_reg, target_register, func_ctx, constant_span);
            }
            ScalarFunc::Char
            | ScalarFunc::Printf
            | ScalarFunc::TestUintEncode
            | ScalarFunc::TestUintDecode
            | ScalarFunc::TestUintAdd
            | ScalarFunc::TestUintSub
            | ScalarFunc::TestUintMul
            | ScalarFunc::TestUintDiv
            | ScalarFunc::TestUintLt
            | ScalarFunc::TestUintEq
            | ScalarFunc::StringReverse
            | ScalarFunc::BooleanToInt
            | ScalarFunc::IntToBoolean
            | ScalarFunc::ValidateIpAddr
            | ScalarFunc::NumericEncode
            | ScalarFunc::NumericDecode
            | ScalarFunc::NumericAdd
            | ScalarFunc::NumericSub
            | ScalarFunc::NumericMul
            | ScalarFunc::NumericDiv
            | ScalarFunc::NumericLt
            | ScalarFunc::NumericEq
            | ScalarFunc::ArrayLength
            | ScalarFunc::ArrayAppend
            | ScalarFunc::ArrayPrepend
            | ScalarFunc::ArrayCat
            | ScalarFunc::ArrayRemove
            | ScalarFunc::ArrayContains
            | ScalarFunc::ArrayPosition
            | ScalarFunc::ArraySlice
            | ScalarFunc::StringToArray
            | ScalarFunc::ArrayToString
            | ScalarFunc::ArrayOverlap
            | ScalarFunc::ArrayContainsAll => {
                let start_reg = self.program.alloc_registers(args.len());
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            ScalarFunc::Coalesce => {
                let args = expect_arguments_min!(args, 2, srf);
                // coalesce function is implemented as a series of not null checks
                // whenever a not null check succeeds, we jump to the end of the series
                let label_coalesce_end = self.program.allocate_label();
                self.schedule_coalesce_arg(
                    referenced_tables,
                    args,
                    target_register,
                    label_coalesce_end,
                    0,
                    constant_span,
                );
            }
            ScalarFunc::LastInsertRowid => {
                let start_reg = self.program.alloc_register();
                self.emit_function_now(start_reg, target_register, func_ctx, constant_span);
            }
            ScalarFunc::Concat => {
                if args.is_empty() {
                    crate::bail_parse_error!(
                        "wrong number of arguments to function {}()",
                        srf.to_string()
                    );
                }
                // Allocate all registers upfront to ensure they're consecutive,
                // since translate_expr may allocate internal registers.
                let start_reg = self.program.alloc_registers(args.len());
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            ScalarFunc::ConcatWs => {
                if args.len() < 2 {
                    crate::bail_parse_error!(
                        "wrong number of arguments to function {}()",
                        srf.to_string()
                    );
                }
                let temp_register = self.program.alloc_registers(args.len() + 1);
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::Function {
                        constant_mask: 0,
                        start_reg: temp_register + 1,
                        dest: temp_register,
                        func: func_ctx,
                    });
                    translator.program.emit_insn(Insn::Copy {
                        src_reg: temp_register,
                        dst_reg: target_register,
                        extra_amount: 0,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.push_translate_args(referenced_tables, args, temp_register + 1);
            }
            ScalarFunc::IfNull => {
                if args.len() != 2 {
                    crate::bail_parse_error!(
                        "{} function requires exactly 2 arguments",
                        srf.to_string()
                    );
                }
                let temp_reg = self.program.alloc_register();
                let before_copy_label = self.program.allocate_label();
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::NotNull {
                        reg: temp_reg,
                        target_pc: before_copy_label,
                    });
                    translator.act(move |translator| {
                        translator
                            .program
                            .preassign_label_to_next_insn(before_copy_label);
                        translator.program.emit_insn(Insn::Copy {
                            src_reg: temp_reg,
                            dst_reg: target_register,
                            extra_amount: 0,
                        });
                        translator.finish_constant_span(constant_span);
                        Ok(())
                    });
                    translator.push_translate_no_constant_opt(
                        referenced_tables,
                        &args[1],
                        temp_reg,
                    );
                    Ok(())
                });
                self.push_translate_no_constant_opt(referenced_tables, &args[0], temp_reg);
            }
            ScalarFunc::Iif => {
                let args = expect_arguments_min!(args, 2, srf);
                let ctx = IifScheduleContext {
                    referenced_tables,
                    args,
                    target_register,
                    condition_reg: self.program.alloc_register(),
                    end_label: self.program.allocate_label(),
                    constant_span,
                };
                self.schedule_iif_pair(ctx, 0);
            }
            ScalarFunc::Glob | ScalarFunc::Like => {
                if args.len() < 2 {
                    crate::bail_parse_error!(
                        "{} function with less than 2 arguments",
                        srf.to_string()
                    );
                }
                let start_reg = self.program.alloc_registers(args.len());
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            ScalarFunc::Abs
            | ScalarFunc::Lower
            | ScalarFunc::Upper
            | ScalarFunc::Length
            | ScalarFunc::OctetLength
            | ScalarFunc::Typeof
            | ScalarFunc::Unicode
            | ScalarFunc::Unistr
            | ScalarFunc::UnistrQuote
            | ScalarFunc::Quote
            | ScalarFunc::RandomBlob
            | ScalarFunc::Sign
            | ScalarFunc::Soundex
            | ScalarFunc::ZeroBlob => {
                let args = expect_arguments_exact!(args, 1, srf);
                let start_reg = self.program.alloc_register();
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            #[cfg(feature = "fs")]
            #[cfg(not(target_family = "wasm"))]
            ScalarFunc::LoadExtension => {
                let args = expect_arguments_exact!(args, 1, srf);
                let start_reg = self.program.alloc_register();
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            ScalarFunc::Random => {
                if !args.is_empty() {
                    crate::bail_parse_error!("{} function with arguments", srf.to_string());
                }
                let start_reg = self.program.alloc_register();
                self.emit_function_now(start_reg, target_register, func_ctx, constant_span);
            }
            ScalarFunc::Date | ScalarFunc::DateTime | ScalarFunc::JulianDay => {
                let start_reg = self.program.alloc_registers(args.len().max(1));
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            ScalarFunc::Substr | ScalarFunc::Substring => {
                if !(args.len() == 2 || args.len() == 3) {
                    crate::bail_parse_error!(
                        "{} function with wrong number of arguments",
                        srf.to_string()
                    )
                }
                let str_reg = self.program.alloc_register();
                let start_reg = self.program.alloc_register();
                let length_reg = self.program.alloc_register();
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::Function {
                        constant_mask: 0,
                        start_reg: str_reg,
                        dest: target_register,
                        func: func_ctx,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                if args.len() == 3 {
                    self.visit(referenced_tables, &args[2], length_reg);
                }
                self.visit(referenced_tables, &args[1], start_reg);
                self.visit(referenced_tables, &args[0], str_reg);
            }
            ScalarFunc::Hex => {
                if args.len() != 1 {
                    crate::bail_parse_error!("hex function must have exactly 1 argument");
                }
                let start_reg = self.program.alloc_register();
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            ScalarFunc::UnixEpoch | ScalarFunc::Time | ScalarFunc::StrfTime => {
                let start_reg = self.program.alloc_registers(args.len().max(1));
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            ScalarFunc::TimeDiff => {
                let args = expect_arguments_exact!(args, 2, srf);
                let start_reg = self.program.alloc_registers(2);
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            ScalarFunc::TotalChanges => {
                if !args.is_empty() {
                    crate::bail_parse_error!("{} function with more than 0 arguments", srf);
                }
                let start_reg = self.program.alloc_register();
                self.emit_function_now(start_reg, target_register, func_ctx, constant_span);
            }
            ScalarFunc::Trim
            | ScalarFunc::LTrim
            | ScalarFunc::RTrim
            | ScalarFunc::Round
            | ScalarFunc::Unhex => {
                let args = expect_arguments_max!(args, 2, srf);
                let start_reg = self.program.alloc_registers(args.len());
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            ScalarFunc::Min => {
                if args.is_empty() {
                    crate::bail_parse_error!("min function with no arguments");
                }
                let start_reg = self.program.alloc_registers(args.len());
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            ScalarFunc::Max => {
                if args.is_empty() {
                    crate::bail_parse_error!("min function with no arguments");
                }
                let start_reg = self.program.alloc_registers(args.len());
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            ScalarFunc::Nullif | ScalarFunc::Instr => {
                if args.len() != 2 {
                    crate::bail_parse_error!("{} function must have two argument", srf.to_string());
                }
                // Allocate both registers first to ensure they're consecutive,
                // since translate_expr may allocate internal registers.
                let first_reg = self.program.alloc_register();
                let second_reg = self.program.alloc_register();
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::Function {
                        constant_mask: 0,
                        start_reg: first_reg,
                        dest: target_register,
                        func: func_ctx,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, &args[1], second_reg);
                self.visit(referenced_tables, &args[0], first_reg);
            }
            ScalarFunc::SqliteVersion | ScalarFunc::TursoVersion | ScalarFunc::SqliteSourceId => {
                if !args.is_empty() {
                    crate::bail_parse_error!("sqlite_version function with arguments");
                }
                let output_register = self.program.alloc_register();
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: output_register,
                    dest: output_register,
                    func: func_ctx,
                });
                self.program.emit_insn(Insn::Copy {
                    src_reg: output_register,
                    dst_reg: target_register,
                    extra_amount: 0,
                });
                self.finish_constant_span(constant_span);
            }
            ScalarFunc::Replace => {
                if args.len() != 3 {
                    crate::bail_parse_error!(
                        "wrong number of arguments to function {}()",
                        srf.to_string()
                    )
                }
                let str_reg = self.program.alloc_register();
                let pattern_reg = self.program.alloc_register();
                let replacement_reg = self.program.alloc_register();
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::Function {
                        constant_mask: 0,
                        start_reg: str_reg,
                        dest: target_register,
                        func: func_ctx,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, &args[2], replacement_reg);
                self.visit(referenced_tables, &args[1], pattern_reg);
                self.visit(referenced_tables, &args[0], str_reg);
            }
            ScalarFunc::Likely => {
                if args.len() != 1 {
                    crate::bail_parse_error!("likely function must have exactly 1 argument");
                }
                self.act(move |translator| {
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, &args[0], target_register);
            }
            ScalarFunc::Likelihood => {
                if args.len() != 2 {
                    crate::bail_parse_error!("likelihood() function must have exactly 2 arguments");
                }
                if let ast::Expr::Literal(ast::Literal::Numeric(ref value)) = args[1].as_ref() {
                    if let Ok(probability) = value.parse::<f64>() {
                        if !(0.0..=1.0).contains(&probability) {
                            crate::bail_parse_error!(
                                "second argument to likelihood() must be a constant between 0.0 and 1.0",
                            );
                        }
                        if !value.contains('.') {
                            crate::bail_parse_error!(
                                "second argument to likelihood() must be a floating point number with decimal point",
                            );
                        }
                    } else {
                        crate::bail_parse_error!(
                            "second argument to likelihood() must be a floating point constant",
                        );
                    }
                } else {
                    crate::bail_parse_error!(
                        "second argument to likelihood() must be a constant between 0.0 and 1.0",
                    );
                }
                self.act(move |translator| {
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, &args[0], target_register);
            }
            ScalarFunc::TableColumnsJsonArray => {
                if args.len() != 1 {
                    crate::bail_parse_error!(
                        "table_columns_json_array() function must have exactly 1 argument",
                    );
                }
                let start_reg = self.program.alloc_register();
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            ScalarFunc::BinRecordJsonObject => {
                if args.len() != 2 {
                    crate::bail_parse_error!(
                        "bin_record_json_object() function must have exactly 2 arguments",
                    );
                }
                let start_reg = self.program.alloc_registers(2);
                self.schedule_function_insn(
                    referenced_tables,
                    args,
                    start_reg,
                    target_register,
                    func_ctx,
                    constant_span,
                );
            }
            ScalarFunc::Attach => {
                crate::bail_parse_error!(
                    "ATTACH should be handled at statement level, not as expression"
                );
            }
            ScalarFunc::Detach => {
                crate::bail_parse_error!(
                    "DETACH should be handled at statement level, not as expression"
                );
            }
            ScalarFunc::Unlikely => {
                if args.len() != 1 {
                    crate::bail_parse_error!("Unlikely function must have exactly 1 argument");
                }
                self.act(move |translator| {
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, &args[0], target_register);
            }
            ScalarFunc::StatInit | ScalarFunc::StatPush | ScalarFunc::StatGet => {
                crate::bail_parse_error!("{} is an internal function used by ANALYZE", srf);
            }
            ScalarFunc::ConnTxnId | ScalarFunc::IsAutocommit => {
                crate::bail_parse_error!("{} is an internal function used by CDC", srf);
            }
            ScalarFunc::UnionValueFunc => {
                // union_value('tag', val): resolve the tag against the
                // target column's union type. The target is set by
                // INSERT/UPDATE/UPSERT before translating the value.
                let args = expect_arguments_exact!(args, 2, srf);
                let tag_name = extract_string_literal(&args[0])?;
                let Some(ref union_td) = self.program.target_union_type else {
                    return Err(crate::LimboError::ParseError(
                        "union_value() can only be used in INSERT/UPDATE targeting a union-typed column".to_string()
                    ));
                };
                let Some((tag_index, variant)) = union_td.find_union_variant(&tag_name) else {
                    return Err(crate::LimboError::ParseError(format!(
                        "unknown variant '{}' in union type '{}'",
                        tag_name, union_td.name
                    )));
                };
                let inner_union_td = self
                    .resolver
                    .schema()
                    .get_type_def_unchecked(&variant.type_name)
                    .filter(|td| td.is_union())
                    .cloned();
                // If the variant's type is itself a union, set
                // target_union_type so nested union_value() resolves
                // against the inner union type, not the outer one.
                let prev = self.program.target_union_type.take();
                self.program.target_union_type = inner_union_td;
                let value_reg = self.program.alloc_register();
                self.act(move |translator| {
                    translator.program.target_union_type = prev;
                    translator.program.emit_insn(Insn::UnionPack {
                        tag_index,
                        value_reg,
                        dest: target_register,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, &args[1], value_reg);
            }
            ScalarFunc::UnionTagFunc => {
                // union_tag(col): resolve col's union type for index-to-name lookup.
                let args = expect_arguments_exact!(args, 1, srf);
                let td = resolve_union_from_column(
                    &args[0],
                    referenced_tables,
                    self.resolver,
                    self.program,
                );
                let tag_names = td
                    .as_ref()
                    .and_then(|td| td.union_def())
                    .map(|ud| Arc::clone(&ud.tag_names))
                    .unwrap_or_else(|| Arc::from(Vec::<String>::new()));
                let src_reg = self.program.alloc_register();
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::UnionTag {
                        src_reg,
                        dest: target_register,
                        tag_names,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, &args[0], src_reg);
            }
            ScalarFunc::UnionExtractFunc => {
                // union_extract(col, 'tag'): resolve col's union type for name-to-index lookup.
                let args = expect_arguments_exact!(args, 2, srf);
                let tag_name = extract_string_literal(&args[1])?;
                let td = resolve_union_from_column(
                    &args[0],
                    referenced_tables,
                    self.resolver,
                    self.program,
                );
                let tag_index = td
                    .as_ref()
                    .and_then(|td| td.resolve_union_tag_index(&tag_name))
                    .ok_or_else(|| {
                        crate::LimboError::ParseError(format!(
                            "cannot resolve union variant '{tag_name}' for union_extract"
                        ))
                    })?;
                let src_reg = self.program.alloc_register();
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::UnionExtract {
                        src_reg,
                        expected_tag: tag_index,
                        dest: target_register,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, &args[0], src_reg);
            }
            ScalarFunc::StructExtractFunc => {
                let args = expect_arguments_exact!(args, 2, srf);
                let field_name = extract_string_literal(&args[1])?;
                let td = resolve_struct_from_expr(
                    &args[0],
                    referenced_tables,
                    self.resolver,
                    self.program,
                );
                let (field_index, _) = td
                    .as_ref()
                    .and_then(|td| td.find_struct_field(&field_name))
                    .ok_or_else(|| {
                        crate::LimboError::ParseError(format!(
                            "cannot resolve struct field '{field_name}' for struct_extract"
                        ))
                    })?;
                let src_reg = self.program.alloc_register();
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::StructField {
                        src_reg,
                        field_index,
                        dest: target_register,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, &args[0], src_reg);
            }
        }
        Ok(())
    }

    fn schedule_coalesce_arg(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        args: &'a [Box<ast::Expr>],
        target_register: usize,
        label_coalesce_end: BranchOffset,
        index: usize,
        constant_span: Option<usize>,
    ) {
        let Some(arg) = args.get(index) else {
            self.act(move |translator| {
                translator
                    .program
                    .preassign_label_to_next_insn(label_coalesce_end);
                translator.finish_constant_span(constant_span);
                Ok(())
            });
            return;
        };

        self.act(move |translator| {
            if index < args.len() - 1 {
                translator.program.emit_insn(Insn::NotNull {
                    reg: target_register,
                    target_pc: label_coalesce_end,
                });
            }
            translator.schedule_coalesce_arg(
                referenced_tables,
                args,
                target_register,
                label_coalesce_end,
                index + 1,
                constant_span,
            );
            Ok(())
        });
        self.push_translate_no_constant_opt(referenced_tables, arg, target_register);
    }

    fn schedule_iif_pair(&mut self, ctx: IifScheduleContext<'a>, index: usize) {
        let args = ctx.args;
        if index + 1 >= args.len() {
            self.act(move |translator| {
                if args.len() % 2 != 0 {
                    translator.act(move |translator| {
                        translator
                            .program
                            .preassign_label_to_next_insn(ctx.end_label);
                        translator.finish_constant_span(ctx.constant_span);
                        Ok(())
                    });
                    translator.push_translate_no_constant_opt(
                        ctx.referenced_tables,
                        args.last().unwrap(),
                        ctx.target_register,
                    );
                } else {
                    translator.program.emit_insn(Insn::Null {
                        dest: ctx.target_register,
                        dest_end: None,
                    });
                    translator
                        .program
                        .preassign_label_to_next_insn(ctx.end_label);
                    translator.finish_constant_span(ctx.constant_span);
                }
                Ok(())
            });
            return;
        }

        let condition_expr = &args[index];
        let value_expr = &args[index + 1];
        let next_check_label = self.program.allocate_label();
        self.act(move |translator| {
            translator.program.emit_insn(Insn::IfNot {
                reg: ctx.condition_reg,
                target_pc: next_check_label,
                jump_if_null: true,
            });
            translator.act(move |translator| {
                translator.program.emit_insn(Insn::Goto {
                    target_pc: ctx.end_label,
                });
                translator
                    .program
                    .preassign_label_to_next_insn(next_check_label);
                translator.schedule_iif_pair(ctx, index + 2);
                Ok(())
            });
            translator.push_translate_no_constant_opt(
                ctx.referenced_tables,
                value_expr,
                ctx.target_register,
            );
            Ok(())
        });
        self.push_translate_no_constant_opt(
            ctx.referenced_tables,
            condition_expr,
            ctx.condition_reg,
        );
    }

    fn schedule_like_base(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        expr: &'a ast::Expr,
        target_register: usize,
    ) -> Result<()> {
        let ast::Expr::Like {
            lhs,
            op,
            rhs,
            escape,
            ..
        } = expr
        else {
            crate::bail_parse_error!("expected Like expression");
        };

        match op {
            ast::LikeOperator::Like | ast::LikeOperator::Glob => {
                let arg_count = if escape.is_some() { 3 } else { 2 };
                let start_reg = self.program.alloc_registers(arg_count);
                let rhs_is_literal = matches!(rhs.as_ref(), ast::Expr::Literal(_));
                let func = match op {
                    ast::LikeOperator::Like => ScalarFunc::Like,
                    ast::LikeOperator::Glob => ScalarFunc::Glob,
                    _ => unreachable!(),
                };
                self.act(move |translator| {
                    let mut constant_mask = 0;
                    if rhs_is_literal {
                        translator.program.mark_last_insn_constant();
                        constant_mask = 1;
                    }
                    translator.program.emit_insn(Insn::Function {
                        constant_mask,
                        start_reg,
                        dest: target_register,
                        func: FuncCtx {
                            func: Func::Scalar(func),
                            arg_count,
                        },
                    });
                    Ok(())
                });
                if let Some(escape) = escape {
                    self.visit(referenced_tables, escape, start_reg + 2);
                }
                self.visit(referenced_tables, rhs, start_reg);
                self.visit(referenced_tables, lhs, start_reg + 1);
            }
            #[cfg(all(feature = "fts", not(target_family = "wasm")))]
            ast::LikeOperator::Match => {
                // Transform MATCH to fts_match():
                // - `col MATCH 'query'` -> `fts_match(col, 'query')`
                // - `(col1, col2) MATCH 'query'` -> `fts_match(col1, col2, 'query')`
                let columns_len = match lhs.as_ref() {
                    ast::Expr::Parenthesized(cols) => cols.len(),
                    _ => 1,
                };
                let arg_count = columns_len + 1;
                let start_reg = self.program.alloc_registers(arg_count);
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::Function {
                        constant_mask: 0,
                        start_reg,
                        dest: target_register,
                        func: FuncCtx {
                            func: Func::Fts(FtsFunc::Match),
                            arg_count,
                        },
                    });
                    Ok(())
                });
                self.visit(referenced_tables, rhs, start_reg + columns_len);
                match lhs.as_ref() {
                    ast::Expr::Parenthesized(cols) => {
                        for (i, col) in cols.iter().enumerate().rev() {
                            self.visit(referenced_tables, col, start_reg + i);
                        }
                    }
                    other => self.visit(referenced_tables, other, start_reg),
                }
            }
            #[cfg(any(not(feature = "fts"), target_family = "wasm"))]
            ast::LikeOperator::Match => {
                crate::bail_parse_error!("MATCH requires the 'fts' feature to be enabled")
            }
            ast::LikeOperator::Regexp => {
                if escape.is_some() {
                    crate::bail_parse_error!("wrong number of arguments to function regexp()");
                }
                let Some(func) = self.resolver.resolve_function("regexp", 2)? else {
                    crate::bail_parse_error!("no such function: regexp");
                };
                let arg_count = 2;
                let start_reg = self.program.alloc_registers(arg_count);
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::Function {
                        constant_mask: 0,
                        start_reg,
                        dest: target_register,
                        func: FuncCtx { func, arg_count },
                    });
                    Ok(())
                });
                self.visit(referenced_tables, lhs, start_reg + 1);
                self.visit(referenced_tables, rhs, start_reg);
            }
        }

        Ok(())
    }

    fn schedule_cast_after_child(
        &mut self,
        type_name: Option<&'a ast::Type>,
        target_register: usize,
        constant_span: Option<usize>,
    ) -> Result<()> {
        if let Some(tn) = type_name {
            if let Some(chain) = self.resolve_type_chain_unchecked(&tn.name)? {
                // Build ty_params from AST TypeSize so parametric types
                // (e.g. numeric(10,2)) get their parameters passed through.
                let ty_params = self.ast_type_params(tn);

                if chain.first().is_some_and(|td| td.is_domain) {
                    // Domains: apply parent encode chain, then validate constraints
                    // on the encoded value (domain CHECK sees the stored representation).
                    self.push_finish_constant_span(constant_span);
                    let constraint_chain = chain.clone();
                    self.act(move |translator| {
                        translator
                            .schedule_domain_cast_constraints(constraint_chain, target_register)?;
                        Ok(())
                    });
                    for td in chain.iter().rev() {
                        if let Some(encode_expr) = td.encode() {
                            let params = self.type_expr_params_from_exprs(td, ty_params.clone());
                            self.schedule_type_expr_with_params(
                                encode_expr,
                                target_register,
                                target_register,
                                params,
                            );
                        }
                    }
                    return Ok(());
                }

                let type_def = chain[0];
                let user_param_count = type_def.user_params().count();
                if user_param_count == 0 || ty_params.len() == user_param_count {
                    if let Some(encode_expr) = type_def.encode() {
                        // CAST to custom type applies only the encode function,
                        // producing the stored representation.
                        // e.g. CAST(42 AS cents) -> 4200
                        self.push_finish_constant_span(constant_span);
                        let params = self.type_expr_params_from_exprs(type_def, ty_params);
                        self.schedule_type_expr_with_params(
                            encode_expr,
                            target_register,
                            target_register,
                            params,
                        );
                    } else {
                        self.finish_constant_span(constant_span);
                    }
                    return Ok(());
                }
                // If the custom type requires parameters but the CAST
                // doesn't provide them (e.g. CAST(x AS NUMERIC) vs
                // CAST(x AS numeric(10,2))), fall through to regular CAST.
            }
        }

        let type_affinity = type_name
            .map(|t| Affinity::affinity(&t.name))
            .unwrap_or(Affinity::Numeric);
        self.program.emit_insn(Insn::Cast {
            reg: target_register,
            affinity: type_affinity,
        });
        self.finish_constant_span(constant_span);
        Ok(())
    }

    /// Emit domain constraint checks for CAST(expr AS domain).
    /// Validates NOT NULL and CHECK constraints from the domain type chain.
    fn schedule_domain_cast_constraints(
        &mut self,
        chain: Vec<&'a TypeDef>,
        reg: usize,
    ) -> Result<()> {
        use crate::error::{SQLITE_CONSTRAINT_CHECK, SQLITE_CONSTRAINT_NOTNULL};

        if chain.iter().any(|td| td.not_null) {
            self.program.emit_insn(Insn::HaltIfNull {
                target_reg: reg,
                err_code: SQLITE_CONSTRAINT_NOTNULL,
                description: format!(
                    "domain {} does not allow null values",
                    chain.first().map(|td| td.name.as_str()).unwrap_or("?")
                ),
            });
        }

        let checks = chain
            .iter()
            .flat_map(|td| {
                td.domain_checks
                    .iter()
                    .enumerate()
                    .map(move |(i, dc)| (*td, i, dc))
            })
            .collect::<Vec<_>>();

        for (td, i, dc) in checks.into_iter().rev() {
            let constraint_name = dc
                .name
                .clone()
                .unwrap_or_else(|| format!("{}_{}", td.name, i));
            let domain_name = td.name.clone();
            let expr_result_reg = self.program.alloc_register();
            let passed_label = self.program.allocate_label();

            self.act(move |translator| {
                translator.program.id_register_overrides.remove("value");
                // NULL result passes CHECK constraints, matching SQLite semantics.
                translator.program.emit_insn(Insn::IsNull {
                    reg: expr_result_reg,
                    target_pc: passed_label,
                });
                translator.program.emit_insn(Insn::If {
                    reg: expr_result_reg,
                    target_pc: passed_label,
                    jump_if_null: false,
                });
                translator.program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_CHECK,
                    description: format!(
                        "value for domain {} violates check constraint \"{}\"",
                        domain_name, constraint_name
                    ),
                    on_error: None,
                    description_reg: None,
                });
                translator
                    .program
                    .preassign_label_to_next_insn(passed_label);
                Ok(())
            });
            self.visit(None, &dc.check, expr_result_reg);
            self.act(move |translator| {
                translator
                    .program
                    .id_register_overrides
                    .insert("value".to_string(), reg);
                Ok(())
            });
        }

        Ok(())
    }

    fn visit_unary_expr(
        &mut self,
        referenced_tables: Option<&'a TableReferences>,
        op: UnaryOperator,
        expr: &'a ast::Expr,
        target_register: usize,
        constant_span: Option<usize>,
    ) -> Result<()> {
        match (op, expr) {
            (UnaryOperator::Positive, expr) => {
                self.act(move |translator| {
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, expr, target_register);
            }
            (UnaryOperator::Negative, ast::Expr::Literal(ast::Literal::Numeric(numeric_value))) => {
                let numeric_value = "-".to_owned() + numeric_value;
                match parse_numeric_literal(&numeric_value)? {
                    Value::Numeric(Numeric::Integer(int_value)) => {
                        self.program.emit_insn(Insn::Integer {
                            value: int_value,
                            dest: target_register,
                        });
                    }
                    Value::Numeric(Numeric::Float(real_value)) => {
                        self.program.emit_insn(Insn::Real {
                            value: real_value.into(),
                            dest: target_register,
                        });
                    }
                    _ => unreachable!(),
                }
                self.finish_constant_span(constant_span);
            }
            (UnaryOperator::Negative, expr) => {
                let reg = self.program.alloc_register();
                self.act(move |translator| {
                    let zero_reg = translator.program.alloc_register();
                    translator.program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: zero_reg,
                    });
                    translator.program.mark_last_insn_constant();
                    translator.program.emit_insn(Insn::Subtract {
                        lhs: zero_reg,
                        rhs: reg,
                        dest: target_register,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, expr, reg);
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Numeric(num_val))) => {
                match parse_numeric_literal(num_val)? {
                    Value::Numeric(Numeric::Integer(int_value)) => {
                        self.program.emit_insn(Insn::Integer {
                            value: !int_value,
                            dest: target_register,
                        });
                    }
                    Value::Numeric(Numeric::Float(real_value)) => {
                        self.program.emit_insn(Insn::Integer {
                            value: !(f64::from(real_value) as i64),
                            dest: target_register,
                        });
                    }
                    _ => unreachable!(),
                }
                self.finish_constant_span(constant_span);
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Null)) => {
                self.program.emit_insn(Insn::Null {
                    dest: target_register,
                    dest_end: None,
                });
                self.finish_constant_span(constant_span);
            }
            (UnaryOperator::BitwiseNot, expr) => {
                let reg = self.program.alloc_register();
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::BitNot {
                        reg,
                        dest: target_register,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, expr, reg);
            }
            (UnaryOperator::Not, expr) => {
                // This is an inefficient implementation for op::NOT, because translate_expr() will emit an Insn::Not,
                // and then we immediately emit an Insn::If/Insn::IfNot for the conditional jump. In reality we would not
                // like to emit the negation instruction Insn::Not at all, since we could just emit the "opposite" jump instruction
                // directly. However, using translate_expr() directly simplifies our conditional jump code for unary expressions,
                // and we'd rather be correct than maximally efficient, for now.
                let reg = self.program.alloc_register();
                self.act(move |translator| {
                    translator.program.emit_insn(Insn::Not {
                        reg,
                        dest: target_register,
                    });
                    translator.finish_constant_span(constant_span);
                    Ok(())
                });
                self.visit(referenced_tables, expr, reg);
            }
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_binary_with_mode(
    program: &mut ProgramBuilder,
    emit_mode: BinaryEmitMode,
    op: &ast::Operator,
    lhs: usize,
    rhs: usize,
    target_register: usize,
    lhs_expr: &Expr,
    rhs_expr: &Expr,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
) -> Result<()> {
    match emit_mode {
        BinaryEmitMode::Value => emit_binary_insn(
            program,
            op,
            lhs,
            rhs,
            target_register,
            lhs_expr,
            rhs_expr,
            referenced_tables,
            Some(resolver),
        ),
        BinaryEmitMode::Condition(condition_metadata) => emit_binary_condition_insn(
            program,
            op,
            lhs,
            rhs,
            target_register,
            lhs_expr,
            rhs_expr,
            referenced_tables,
            condition_metadata,
            Some(resolver),
        ),
    }
}

/// Translate an expression into bytecode.
#[turso_macros::trace_stack]
pub fn translate_expr<'a, 'r>(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&'a TableReferences>,
    expr: &'a ast::Expr,
    target_register: usize,
    resolver: &'a Resolver<'r>,
) -> Result<usize> {
    let mut translator = IterativeExprTranslator::new(program, resolver);
    translator.translate(referenced_tables, expr, target_register)
}
