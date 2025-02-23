use crate::{
    vdbe::{builder::ProgramBuilder, insn::Insn, BranchOffset},
    LimboError, Result,
};
use limbo_sqlite3_parser::ast::{self, Literal};

pub fn emit_values(program: &mut ProgramBuilder, values: &[Vec<ast::Expr>]) -> Result<()> {
    let goto_target = program.allocate_label();
    program.emit_insn(Insn::Init {
        target_pc: goto_target,
    });

    let start_reg = 1;
    let first_row_len = values[0].len();

    for row in values {
        if row.len() != first_row_len {
            return Err(LimboError::ParseError(
                "all VALUES rows must have the same number of values".into(),
            ));
        }

        for (i, expr) in row.iter().enumerate() {
            let reg = start_reg + i;
            match expr {
                ast::Expr::Literal(lit) => match lit {
                    Literal::String(s) => {
                        let s = &s[1..s.len()-1];
                        let s = s.replace("''", "'");
                        program.emit_insn(Insn::String8 {
                            value: s,
                            dest: reg,
                        });
                    }
                    Literal::Numeric(num) => {
                        if let Ok(int_val) = num.parse::<i64>() {
                            program.emit_insn(Insn::Integer {
                                value: int_val,
                                dest: reg,
                            });
                        } else {
                            let float_val = num.parse::<f64>()?;
                            program.emit_insn(Insn::Real {
                                value: float_val,
                                dest: reg,
                            });
                        }
                    }
                    Literal::Null => {
                        program.emit_insn(Insn::Null {
                            dest: reg,
                            dest_end: None,
                        });
                    }
                    _ => {
                        return Err(LimboError::ParseError(
                            "unsupported literal type in VALUES".into(),
                        ))
                    }
                },
                ast::Expr::Unary(op, expr) => {
                    match (&op, expr.as_ref()) {
                        (ast::UnaryOperator::Negative | ast::UnaryOperator::Positive, ast::Expr::Literal(Literal::Numeric(numeric_value))) => {
                            let multiplier = if let ast::UnaryOperator::Negative = op { -1 } else { 1 };
                            
                            // Special case: check for negating i64::MAX+1 to get i64::MIN
                            if multiplier == -1 && numeric_value == "9223372036854775808" {
                                program.emit_insn(Insn::Integer {
                                    value: i64::MIN,
                                    dest: reg,
                                });
                            } else {
                                let maybe_int = numeric_value.parse::<i64>();
                                if let Ok(value) = maybe_int {
                                    program.emit_insn(Insn::Integer {
                                        value: value * multiplier,
                                        dest: reg,
                                    });
                                } else {
                                    let value = numeric_value.parse::<f64>()?;
                                    program.emit_insn(Insn::Real {
                                        value: value * multiplier as f64,
                                        dest: reg,
                                    });
                                }
                            }
                        },
                        _ => {
                            return Err(LimboError::ParseError(
                                "VALUES only supports literal values and unary on numbers".into(),
                            ))
                        }
                    }
                },
                _ => {
                    return Err(LimboError::ParseError(
                        "VALUES only supports literal values and unary operations".into(),
                    ))
                }
            }
        }

        program.emit_insn(Insn::ResultRow {
            start_reg,
            count: first_row_len,
        });
    }

    program.emit_insn(Insn::Halt {
        err_code: 0,
        description: String::new(),
    });

    program.preassign_label_to_next_insn(goto_target);
    program.emit_insn(Insn::Goto {
        target_pc: BranchOffset::Offset(1),
    });

    Ok(())
}