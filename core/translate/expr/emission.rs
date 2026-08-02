use crate::alloc::TursoIteratorExt;
use crate::vdbe::insn::Insn;
use crate::{
    functions::datetime, util::parse_numeric_literal, vdbe::builder::ProgramBuilder, Numeric,
    Result, Value,
};
use turso_parser::ast;

use super::sanitize_string;

pub fn emit_literal(
    program: &mut ProgramBuilder,
    literal: &ast::Literal,
    target_register: usize,
) -> Result<usize> {
    match literal {
        ast::Literal::Numeric(value) => {
            match parse_numeric_literal(value)? {
                Value::Numeric(Numeric::Integer(value)) => program.emit_insn(Insn::Integer {
                    value,
                    dest: target_register,
                }),
                Value::Numeric(Numeric::Float(value)) => program.emit_insn(Insn::Real {
                    value: value.into(),
                    dest: target_register,
                }),
                _ => unreachable!("numeric parser returned a non-numeric value"),
            }
            Ok(target_register)
        }
        ast::Literal::String(value) => {
            program.emit_insn(Insn::String8 {
                value: sanitize_string(value),
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::Blob(value) => {
            let bytes = ast::blob_literal_hex(value)
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    let byte = std::str::from_utf8(pair).expect("parser validated blob UTF-8");
                    u8::from_str_radix(byte, 16).expect("parser validated blob hex")
                })
                .try_collect()?;
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
        ast::Literal::True | ast::Literal::False => {
            program.emit_insn(Insn::Integer {
                value: i64::from(matches!(literal, ast::Literal::True)),
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::CurrentDate => {
            program.emit_insn(Insn::String8 {
                value: datetime::exec_date::<&[_; 0], std::slice::Iter<'_, Value>, &Value>(&[])
                    .to_string(),
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::CurrentTime => {
            program.emit_insn(Insn::String8 {
                value: datetime::exec_time::<&[_; 0], std::slice::Iter<'_, Value>, &Value>(&[])
                    .to_string(),
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::CurrentTimestamp => {
            program.emit_insn(
                Insn::String8 {
                    value: datetime::exec_datetime_full::<
                        &[_; 0],
                        std::slice::Iter<'_, Value>,
                        &Value,
                    >(&[])
                    .to_string(),
                    dest: target_register,
                },
            );
            Ok(target_register)
        }
    }
}
