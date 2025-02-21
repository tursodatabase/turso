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
                        program.emit_insn(Insn::String8 {
                            value: s.clone(),
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
                _ => {
                    return Err(LimboError::ParseError(
                        "VALUES only supports literal values".into(),
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
