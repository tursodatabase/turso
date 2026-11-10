use super::*;

pub struct FunctionCompileContext<'a, 'resolver> {
    pub(crate) program: &'a mut ProgramBuilder,
    pub(crate) args: &'a [Box<ast::Expr>],
    pub(crate) referenced_tables: Option<&'a TableReferences>,
    pub(crate) resolver: &'a Resolver<'resolver>,
    pub(crate) target_register: usize,
    pub(crate) func_ctx: FuncCtx,
}

impl FunctionCompileContext<'_, '_> {
    pub fn compile_default(self) -> Result<usize> {
        translate_function(
            self.program,
            self.args,
            self.referenced_tables,
            self.resolver,
            self.target_register,
            self.func_ctx,
        )
    }
    pub(crate) fn coalesce(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;
        let srf = "coalesce";

        let args = expect_arguments_min!(args, 2, srf);

        // coalesce function is implemented as a series of not null checks
        // whenever a not null check succeeds, we jump to the end of the series
        let label_coalesce_end = program.allocate_label();
        let values_type = untyped_values_type(
            args.iter().map(|arg| arg.as_ref()),
            referenced_tables,
            resolver,
        )?;
        for (index, arg) in args.iter().enumerate() {
            let reg = translate_returned_value(
                program,
                referenced_tables,
                arg,
                values_type.as_ref(),
                target_register,
                resolver,
            )?;
            if index < args.len() - 1 {
                program.emit_insn(Insn::NotNull {
                    reg,
                    target_pc: label_coalesce_end,
                });
            }
        }
        program.preassign_label_to_next_insn(label_coalesce_end);

        Ok(target_register)
    }
    pub(crate) fn ifnull(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;
        let srf = "ifnull";

        if args.len() != 2 {
            crate::bail_parse_error!("{} function requires exactly 2 arguments", srf.to_string());
        }

        let values_type = untyped_values_type(
            args.iter().map(|arg| arg.as_ref()),
            referenced_tables,
            resolver,
        )?;
        let temp_reg = program.alloc_register();
        translate_returned_value(
            program,
            referenced_tables,
            &args[0],
            values_type.as_ref(),
            temp_reg,
            resolver,
        )?;
        let before_copy_label = program.allocate_label();
        program.emit_insn(Insn::NotNull {
            reg: temp_reg,
            target_pc: before_copy_label,
        });

        translate_returned_value(
            program,
            referenced_tables,
            &args[1],
            values_type.as_ref(),
            temp_reg,
            resolver,
        )?;
        program.preassign_label_to_next_insn(before_copy_label);
        program.emit_insn(Insn::Copy {
            src_reg: temp_reg,
            dst_reg: target_register,
            extra_amount: 0,
        });

        Ok(target_register)
    }
    pub(crate) fn iif(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;
        let srf = "iif";

        let args = expect_arguments_min!(args, 2, srf);

        let iif_end_label = program.allocate_label();
        let condition_reg = program.alloc_register();
        let values_type = untyped_values_type(
            iif_values(args).map(|arg| arg.as_ref()),
            referenced_tables,
            resolver,
        )?;

        for pair in args.chunks_exact(2) {
            let condition_expr = &pair[0];
            let value_expr = &pair[1];
            let next_check_label = program.allocate_label();

            translate_expr_no_constant_opt(
                program,
                referenced_tables,
                condition_expr,
                condition_reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;

            program.emit_insn(Insn::IfNot {
                reg: condition_reg,
                target_pc: next_check_label,
                jump_if_null: true,
            });

            translate_returned_value(
                program,
                referenced_tables,
                value_expr,
                values_type.as_ref(),
                target_register,
                resolver,
            )?;
            program.emit_insn(Insn::Goto {
                target_pc: iif_end_label,
            });

            program.preassign_label_to_next_insn(next_check_label);
        }

        if args.len() % 2 != 0 {
            translate_returned_value(
                program,
                referenced_tables,
                args.last().unwrap(),
                values_type.as_ref(),
                target_register,
                resolver,
            )?;
        } else {
            program.emit_insn(Insn::Null {
                dest: target_register,
                dest_end: None,
            });
        }

        program.preassign_label_to_next_insn(iif_end_label);
        Ok(target_register)
    }
    pub(crate) fn likely(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;

        if args.len() != 1 {
            crate::bail_parse_error!("likely function must have exactly 1 argument",);
        }
        translate_expr(
            program,
            referenced_tables,
            &args[0],
            target_register,
            resolver,
        )?;
        Ok(target_register)
    }
    pub(crate) fn unlikely(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;

        if args.len() != 1 {
            crate::bail_parse_error!("Unlikely function must have exactly 1 argument",);
        }
        translate_expr(
            program,
            referenced_tables,
            &args[0],
            target_register,
            resolver,
        )?;

        Ok(target_register)
    }
    pub(crate) fn likelihood(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;

        if args.len() != 2 {
            crate::bail_parse_error!("likelihood() function must have exactly 2 arguments",);
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
        translate_expr(
            program,
            referenced_tables,
            &args[0],
            target_register,
            resolver,
        )?;
        Ok(target_register)
    }
}
