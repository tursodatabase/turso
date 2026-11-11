use super::{FunctionCompileContext, IntrinsicFunction};
use crate::scalar::{FunctionArity, ResultType, ScalarFunctionMetadata};
use crate::Result;

#[derive(Debug)]
pub(crate) struct Coalesce;

impl IntrinsicFunction for Coalesce {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.coalesce()
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "coalesce",
            arity: FunctionArity::AtLeast(2),
            deterministic: true,
            can_mask_nulls: true,
            result_type: ResultType::OneOfTheArguments,
        }
    }
}

#[derive(Debug)]
pub(crate) struct IfNull;

impl IntrinsicFunction for IfNull {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.ifnull()
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "ifnull",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: true,
            result_type: ResultType::OneOfTheArguments,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Iif;

impl IntrinsicFunction for Iif {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.iif()
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "iif",
            arity: FunctionArity::AtLeast(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::OneOfTheIifValues,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Likely;

impl IntrinsicFunction for Likely {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.likely()
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "likely",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::FirstArgument,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Unlikely;

impl IntrinsicFunction for Unlikely {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.unlikely()
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "unlikely",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::FirstArgument,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Likelihood;

impl IntrinsicFunction for Likelihood {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.likelihood()
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "likelihood",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::FirstArgument,
        }
    }
}
