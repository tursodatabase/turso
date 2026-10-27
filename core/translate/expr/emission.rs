use super::sanitize_string;
use crate::alloc::TursoIteratorExt;
use crate::function::{Func, FuncCtx, ScalarFunc};
use crate::util::parse_numeric_literal;
use crate::vdbe::{builder::ProgramBuilder, insn::Insn};
use crate::{Numeric, Result, Value};
use turso_parser::ast;

/// Emit literal values - shared between regular and RETURNING expression evaluation
pub fn emit_literal(
    program: &mut ProgramBuilder,
    literal: &ast::Literal,
    target_register: usize,
) -> Result<usize> {
    match literal {
        ast::Literal::Numeric(val) => {
            match parse_numeric_literal(val)? {
                Value::Numeric(Numeric::Integer(int_value)) => {
                    program.emit_insn(Insn::Integer {
                        value: int_value,
                        dest: target_register,
                    });
                }
                Value::Numeric(Numeric::Float(real_value)) => {
                    program.emit_insn(Insn::Real {
                        value: real_value.into(),
                        dest: target_register,
                    });
                }
                _ => unreachable!(),
            }
            Ok(target_register)
        }
        ast::Literal::String(s) => {
            program.emit_insn(Insn::String8 {
                value: sanitize_string(s),
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::Blob(s) => {
            let bytes = ast::blob_literal_bytes(s).try_collect()?;
            program.emit_insn(Insn::Blob {
                value: bytes,
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::Keyword(_) => {
            crate::bail_parse_error!("Keyword in WHERE clause is not supported")
        }
        ast::Literal::Null => {
            program.emit_insn(Insn::Null {
                dest: target_register,
                dest_end: None,
            });
            Ok(target_register)
        }
        ast::Literal::True => {
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::False => {
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: target_register,
            });
            Ok(target_register)
        }
        // The time is read when the statement runs, not when it is prepared.
        ast::Literal::CurrentDate => {
            emit_current_time_function(program, ScalarFunc::Date, target_register)
        }
        ast::Literal::CurrentTime => {
            emit_current_time_function(program, ScalarFunc::Time, target_register)
        }
        ast::Literal::CurrentTimestamp => {
            emit_current_time_function(program, ScalarFunc::DateTime, target_register)
        }
    }
}

fn emit_current_time_function(
    program: &mut ProgramBuilder,
    func: ScalarFunc,
    target_register: usize,
) -> Result<usize> {
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: target_register,
        dest: target_register,
        func: FuncCtx {
            func: Func::Scalar(func),
            arg_count: 0,
        },
    });
    Ok(target_register)
}
