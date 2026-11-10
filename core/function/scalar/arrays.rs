use super::ScalarState;
use super::{
    FunctionArgs, FunctionArity, FunctionContext, ResultType, ReturnType, ScalarFunction,
    ScalarFunctionMetadata,
};
use crate::vdbe::array;
use crate::IOResult;
use crate::{Result, Value};

#[derive(Debug)]
pub(crate) struct ArrayLength;

impl ScalarFunction for ArrayLength {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array_length",
            arity: FunctionArity::OneOf(&[1, 2]),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        let dim = args
            .get_optional(1)
            .map_or(1, |value| value.as_int().unwrap_or(0));
        Ok(IOResult::Done(
            array::compute_array_length_at_dim(args.get(0), dim)
                .map_or(Value::Null, Value::from_i64),
        ))
    }
}

#[derive(Debug)]
pub(crate) struct ArrayAppend;

impl ScalarFunction for ArrayAppend {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array_append",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Array),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        array::exec_array_append(args.get(0), args.get(1)).map(IOResult::Done)
    }
}

#[derive(Debug)]
pub(crate) struct ArrayCat;

impl ScalarFunction for ArrayCat {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array_cat",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Array),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        array::exec_array_cat(args.get(0), args.get(1)).map(IOResult::Done)
    }
}

#[derive(Debug)]
pub(crate) struct ArrayRemove;

impl ScalarFunction for ArrayRemove {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array_remove",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Array),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        array::exec_array_remove(args.get(0), args.get(1)).map(IOResult::Done)
    }
}

#[derive(Debug)]
pub(crate) struct ArrayContains;

impl ScalarFunction for ArrayContains {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array_contains",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(array::exec_array_contains(
            args.get(0),
            args.get(1),
        )))
    }
}

#[derive(Debug)]
pub(crate) struct ArrayPosition;

impl ScalarFunction for ArrayPosition {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array_position",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(array::exec_array_position(
            args.get(0),
            args.get(1),
        )))
    }
}

#[derive(Debug)]
pub(crate) struct ArrayOverlap;

impl ScalarFunction for ArrayOverlap {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array_overlap",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(array::exec_array_overlap(
            args.get(0),
            args.get(1),
        )))
    }
}

#[derive(Debug)]
pub(crate) struct ArrayContainsAll;

impl ScalarFunction for ArrayContainsAll {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array_contains_all",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(array::exec_array_contains_all(
            args.get(0),
            args.get(1),
        )))
    }
}

#[derive(Debug)]
pub(crate) struct ArrayPrepend;

impl ScalarFunction for ArrayPrepend {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array_prepend",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Array),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        array::exec_array_prepend(args.get(1), args.get(0)).map(IOResult::Done)
    }
}

#[derive(Debug)]
pub(crate) struct ArraySlice;

impl ScalarFunction for ArraySlice {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array_slice",
            arity: FunctionArity::Exact(3),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Array),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        array::exec_array_slice(args.get(0), args.get(1), args.get(2)).map(IOResult::Done)
    }
}

#[derive(Debug)]
pub(crate) struct StringToArray;

impl ScalarFunction for StringToArray {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "string_to_array",
            arity: FunctionArity::OneOf(&[2, 3]),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::TextArray),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        array::exec_string_to_array(args.get(0), args.get(1), args.get_optional(2))
            .map(IOResult::Done)
    }
}

#[derive(Debug)]
pub(crate) struct ArrayToString;

impl ScalarFunction for ArrayToString {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array_to_string",
            arity: FunctionArity::OneOf(&[2, 3]),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(array::exec_array_to_string(
            args.get(0),
            args.get(1),
            args.get_optional(2),
        )))
    }
}
