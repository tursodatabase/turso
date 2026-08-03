//! Shared RETURNING projection from the row image owned by a DML write loop.

use crate::{
    translate::semantic::hir,
    vdbe::{builder::ProgramBuilder, insn::Insn},
};

use super::{
    emit_expression_for_dml, PhysicalPlan, PhysicalQueryError, RegisterRange, RuntimeBindings,
};

pub(crate) fn emit_returning_values<'document>(
    plan: &PhysicalPlan<'document>,
    program: &mut ProgramBuilder,
    bindings: &mut RuntimeBindings<'document>,
    returning: &hir::Returning,
) -> Result<RegisterRange, PhysicalQueryError> {
    if returning.outputs.is_empty() {
        return Err(PhysicalQueryError::Invalid("RETURNING has no outputs"));
    }
    let result = RegisterRange::new(
        program.alloc_registers(returning.outputs.len()),
        returning.outputs.len(),
    );
    for (position, output) in returning.outputs.iter().enumerate() {
        let value = emit_expression_for_dml(plan, program, bindings, &output.expr)?;
        if value.width != 1 {
            return Err(PhysicalQueryError::Invalid(
                "RETURNING output is not scalar",
            ));
        }
        program.emit_insn(Insn::Copy {
            src_reg: value.first.0,
            dst_reg: result.first.0 + position,
            extra_amount: 0,
        });
    }
    Ok(result)
}

pub(crate) fn emit_returning_result(program: &mut ProgramBuilder, result: RegisterRange) {
    program.emit_insn(Insn::ResultRow {
        start_reg: result.first.0,
        count: result.width,
    });
}
