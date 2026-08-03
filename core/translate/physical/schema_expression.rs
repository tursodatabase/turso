//! Physical lowering for closed stored-expression batches.

use std::fmt;

use crate::vdbe::builder::ProgramBuilder;

use super::{
    ExpressionEmitter, PhysicalExpressionError, PhysicalPlan, PhysicalRoot, RegisterRange,
    RootRuntimeInputs, RuntimeBindingError, RuntimeBindings,
};

#[derive(Debug)]
pub(crate) enum PhysicalSchemaExpressionError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Unsupported(&'static str),
}

impl fmt::Display for PhysicalSchemaExpressionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::Expression(error) => error.fmt(formatter),
            Self::Unsupported(message) => {
                write!(
                    formatter,
                    "physical schema expression is not emitted: {message}"
                )
            }
        }
    }
}

impl std::error::Error for PhysicalSchemaExpressionError {}

impl From<RuntimeBindingError> for PhysicalSchemaExpressionError {
    fn from(error: RuntimeBindingError) -> Self {
        Self::Runtime(error)
    }
}

impl From<PhysicalExpressionError> for PhysicalSchemaExpressionError {
    fn from(error: PhysicalExpressionError) -> Self {
        Self::Expression(error)
    }
}

pub(crate) fn emit_root_schema_expressions(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
) -> Result<Vec<RegisterRange>, PhysicalSchemaExpressionError> {
    let root = match plan.root {
        PhysicalRoot::SchemaExpressions(root) => root,
        _ => {
            return Err(PhysicalSchemaExpressionError::Unsupported(
                "non-schema-expression HIR root",
            ))
        }
    };
    let mut bindings = RuntimeBindings::new(plan.document, plan.document.snapshot)?;
    inputs.apply(&mut bindings)?;
    // Requiring the source binding here catches callers that forgot to map the
    // positional schema source before any bytecode is emitted.
    bindings.source(root.source)?;

    root.expressions
        .iter()
        .map(|expression| {
            ExpressionEmitter::new(program, &mut bindings)
                .emit_new(expression)
                .map_err(PhysicalSchemaExpressionError::Expression)
        })
        .collect()
}

/// Emit one member of a closed schema-expression batch into a caller-owned
/// register. This lets a partial-index predicate run before its key
/// expressions, so rows excluded by the predicate cannot raise key errors.
pub(crate) fn emit_root_schema_expression_into(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
    expression_index: usize,
    target_register: usize,
) -> Result<(), PhysicalSchemaExpressionError> {
    let root = match plan.root {
        PhysicalRoot::SchemaExpressions(root) => root,
        _ => {
            return Err(PhysicalSchemaExpressionError::Unsupported(
                "non-schema-expression HIR root",
            ))
        }
    };
    let expression = root.expressions.get(expression_index).ok_or(
        PhysicalSchemaExpressionError::Unsupported("schema-expression index is outside the root"),
    )?;
    let mut bindings = RuntimeBindings::new(plan.document, plan.document.snapshot)?;
    inputs.apply(&mut bindings)?;
    bindings.source(root.source)?;
    ExpressionEmitter::new(program, &mut bindings)
        .emit_into(expression, RegisterRange::new(target_register, 1))?;
    Ok(())
}
