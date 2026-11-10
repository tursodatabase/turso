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

impl FunctionCompileContext<'_, '_> {
    pub(crate) fn array(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;

        translate_variadic_insn!(
            program,
            referenced_tables,
            resolver,
            args,
            target_register,
            MakeArray
        )
    }
    pub(crate) fn array_element(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;

        translate_fixed_insn!(program, referenced_tables, resolver, args, target_register,
                                [array_reg <- 0, index_reg <- 1],
                                Insn::ArrayElement { array_reg, index_reg, dest: target_register })
    }
    pub(crate) fn array_set_element(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;

        translate_fixed_insn!(program, referenced_tables, resolver, args, target_register,
                                [array_reg <- 0, index_reg <- 1, value_reg <- 2],
                                Insn::ArraySetElement { array_reg, index_reg, value_reg, dest: target_register })
    }
    pub(crate) fn struct_pack(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;

        translate_variadic_insn!(
            program,
            referenced_tables,
            resolver,
            args,
            target_register,
            MakeArray
        )
    }
    pub(crate) fn struct_extract(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;
        let srf = "struct_extract";

        let args = expect_arguments_exact!(args, 2, srf);
        let field_name = extract_string_literal(&args[1])?;
        let td = resolve_struct_from_expr(&args[0], referenced_tables, resolver);
        let (field_index, _) = td
            .as_ref()
            .and_then(|td| td.find_struct_field(&field_name))
            .ok_or_else(|| {
                crate::LimboError::ParseError(format!(
                    "cannot resolve struct field '{field_name}' for struct_extract"
                ))
            })?;
        translate_fixed_insn!(program, referenced_tables, resolver, args, target_register,
                                [src_reg <- 0],
                                Insn::StructField { src_reg, field_index, dest: target_register })
    }
    pub(crate) fn union_value(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;
        let srf = "union_value";

        let args = expect_arguments_exact!(args, 2, srf);
        let tag_name = extract_string_literal(&args[0])?;
        // union_value('tag', val): resolve the tag against the
        // target column's union type. The target is set by
        // INSERT/UPDATE/UPSERT before translating the value.
        let Some(ref union_td) = program.target_union_type else {
            return Err(crate::LimboError::ParseError(
                "union_value() can only be used in INSERT/UPDATE targeting a union-typed column"
                    .to_string(),
            ));
        };
        let Some((tag_index, variant)) = union_td.find_union_variant(&tag_name) else {
            return Err(crate::LimboError::ParseError(format!(
                "unknown variant '{}' in union type '{}'",
                tag_name, union_td.name
            )));
        };
        // If the variant's type is itself a union, set
        // target_union_type so nested union_value() resolves
        // against the inner union type, not the outer one.
        let inner_union_td = resolver
            .schema()
            .get_type_def_unchecked(&variant.type_name)
            .filter(|td| td.is_union())
            .cloned();
        let prev = program.target_union_type.take();
        program.target_union_type = inner_union_td;
        let value_reg = program.alloc_register();
        let result = translate_expr(program, referenced_tables, &args[1], value_reg, resolver);
        program.target_union_type = prev;
        result?;
        program.emit_insn(Insn::UnionPack {
            tag_index,
            value_reg,
            dest: target_register,
        });
        Ok(target_register)
    }
    pub(crate) fn union_tag(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;
        let srf = "union_tag";

        // union_tag(col): resolve col's union type for index→name lookup.
        let args = expect_arguments_exact!(args, 1, srf);
        let td = resolve_union_from_column(&args[0], referenced_tables, resolver);
        let tag_names = td
            .as_ref()
            .and_then(|td| td.union_def())
            .map(|ud| Arc::clone(&ud.tag_names))
            .unwrap_or_else(|| Arc::from(Vec::<String>::new()));
        translate_fixed_insn!(program, referenced_tables, resolver, args, target_register,
                                [src_reg <- 0],
                                Insn::UnionTag { src_reg, dest: target_register, tag_names })
    }
    pub(crate) fn union_extract(self) -> Result<usize> {
        let Self {
            program,
            args,
            referenced_tables,
            resolver,
            target_register,
            func_ctx: _,
        } = self;
        let srf = "union_extract";

        let args = expect_arguments_exact!(args, 2, srf);
        let tag_name = extract_string_literal(&args[1])?;
        // union_extract(col, 'tag'): resolve col's union type for name→index.
        let td = resolve_union_from_column(&args[0], referenced_tables, resolver);
        let tag_index = td
            .as_ref()
            .and_then(|td| td.resolve_union_tag_index(&tag_name))
            .ok_or_else(|| {
                crate::LimboError::ParseError(format!(
                    "cannot resolve union variant '{tag_name}' for union_extract"
                ))
            })?;
        translate_fixed_insn!(program, referenced_tables, resolver, args, target_register,
                                [src_reg <- 0],
                                Insn::UnionExtract { src_reg, expected_tag: tag_index, dest: target_register })
    }
}
