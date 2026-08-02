//! Trigger-specific lowering for explicit OLD and NEW HIR sources.

use std::fmt;

use crate::vdbe::{builder::ProgramBuilder, insn::Insn, BranchOffset};

use super::{
    ExpressionEmitter, PhysicalExpressionError, PhysicalPlan, PhysicalRoot, RootRuntimeInputs,
    RuntimeBindingError, RuntimeBindings,
};

#[derive(Debug)]
pub(crate) enum PhysicalTriggerError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Invalid(&'static str),
}

impl fmt::Display for PhysicalTriggerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::Expression(error) => error.fmt(formatter),
            Self::Invalid(message) => write!(formatter, "invalid physical trigger: {message}"),
        }
    }
}

impl std::error::Error for PhysicalTriggerError {}

impl From<RuntimeBindingError> for PhysicalTriggerError {
    fn from(error: RuntimeBindingError) -> Self {
        Self::Runtime(error)
    }
}

impl From<PhysicalExpressionError> for PhysicalTriggerError {
    fn from(error: PhysicalExpressionError) -> Self {
        Self::Expression(error)
    }
}

/// Emit a trigger WHEN predicate and branch when it is false or NULL.
///
/// OLD and NEW are ordinary root inputs here. The caller decides whether they
/// came from parent registers, subprogram parameters, or a test fixture.
pub(crate) fn emit_trigger_predicate(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
    false_target: BranchOffset,
) -> Result<(), PhysicalTriggerError> {
    let predicate = match &plan.root {
        PhysicalRoot::TriggerPredicate(predicate) => *predicate,
        _ => return Err(PhysicalTriggerError::Invalid("non-predicate HIR root")),
    };
    let mut bindings = RuntimeBindings::new(plan.document, plan.document.snapshot)?;
    inputs.apply(&mut bindings)?;
    let result = ExpressionEmitter::new(program, &mut bindings).emit_new(&predicate.expression)?;
    if result.width != 1 {
        return Err(PhysicalTriggerError::Invalid(
            "WHEN predicate result is not scalar",
        ));
    }
    program.emit_insn(Insn::IfNot {
        reg: result.first.0,
        target_pc: false_target,
        jump_if_null: true,
    });
    Ok(())
}
