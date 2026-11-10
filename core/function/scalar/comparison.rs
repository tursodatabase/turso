use super::ScalarState;
use super::{
    ArgumentTranslation, FunctionArgs, FunctionArity, FunctionContext, ResultType, ScalarFunction,
    ScalarFunctionMetadata,
};
use crate::IOResult;
use crate::{Result, Value};

#[derive(Debug)]
pub(crate) struct Min;

impl ScalarFunction for Min {
    fn argument_translation(&self) -> ArgumentTranslation {
        ArgumentTranslation::ComparedValues
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "min",
            arity: FunctionArity::AtLeast(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::OneOfTheArguments,
        }
    }
    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Value::exec_min(args.iter(), |a, b| Ok(context.compare(a, b))).map(IOResult::Done)
    }
}

#[derive(Debug)]
pub(crate) struct Max;

impl ScalarFunction for Max {
    fn argument_translation(&self) -> ArgumentTranslation {
        ArgumentTranslation::ComparedValues
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "max",
            arity: FunctionArity::AtLeast(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::OneOfTheArguments,
        }
    }
    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Value::exec_max(args.iter(), |a, b| Ok(context.compare(a, b))).map(IOResult::Done)
    }
}

#[derive(Debug)]
pub(crate) struct Nullif;

impl ScalarFunction for Nullif {
    fn argument_translation(&self) -> ArgumentTranslation {
        ArgumentTranslation::ComparedPair
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "nullif",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::FirstArgument,
        }
    }
    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        args.get(0)
            .exec_nullif(args.get(1), |a, b| Ok(context.compare(a, b)))
            .map(IOResult::Done)
    }
}
