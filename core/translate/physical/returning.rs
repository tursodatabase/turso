//! Shared RETURNING projection from the row image owned by a DML write loop.

use crate::{
    translate::semantic::hir,
    vdbe::{builder::ProgramBuilder, insn::Insn},
};

use super::{ExpressionEmitter, PhysicalExpressionError, RegisterRange, RuntimeBindings};

pub(crate) fn emit_returning_values(
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'_>,
    returning: &hir::Returning,
) -> Result<RegisterRange, PhysicalExpressionError> {
    if returning.outputs.is_empty() {
        return Err(PhysicalExpressionError::Invalid("RETURNING has no outputs"));
    }
    let result = RegisterRange::new(
        program.alloc_registers(returning.outputs.len()),
        returning.outputs.len(),
    );
    for (position, output) in returning.outputs.iter().enumerate() {
        ExpressionEmitter::new(program, bindings).emit_into(
            &output.expr,
            RegisterRange::new(result.first.0 + position, 1),
        )?;
    }
    Ok(result)
}

pub(crate) fn emit_returning_result(program: &mut ProgramBuilder, result: RegisterRange) {
    program.emit_insn(Insn::ResultRow {
        start_reg: result.first.0,
        count: result.width,
    });
}
