use super::{FunctionCompileContext, IntrinsicFunction};
use crate::scalar::{FunctionArity, ResultType, ReturnType, ScalarFunctionMetadata};
use crate::Result;

#[derive(Debug)]
pub(crate) struct Array;

impl IntrinsicFunction for Array {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.array()
    }
    fn is_internal(&self) -> bool {
        true
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array",
            arity: FunctionArity::Variadic,
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::ArrayOfTheArguments,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ArrayElement;

impl IntrinsicFunction for ArrayElement {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.array_element()
    }
    fn is_internal(&self) -> bool {
        true
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array_element",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::ElementOfTheFirstArgument,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ArraySetElement;

impl IntrinsicFunction for ArraySetElement {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.array_set_element()
    }
    fn is_internal(&self) -> bool {
        true
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "array_set_element",
            arity: FunctionArity::Exact(3),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Array),
        }
    }
}

#[derive(Debug)]
pub(crate) struct StructPack;

impl IntrinsicFunction for StructPack {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.struct_pack()
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "struct_pack",
            arity: FunctionArity::Variadic,
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Unknown,
        }
    }
}

#[derive(Debug)]
pub(crate) struct StructExtractFunc;

impl IntrinsicFunction for StructExtractFunc {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.struct_extract()
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "struct_extract",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Unknown,
        }
    }
}

#[derive(Debug)]
pub(crate) struct UnionValueFunc;

impl IntrinsicFunction for UnionValueFunc {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.union_value()
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "union_value",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Unknown,
        }
    }
}

#[derive(Debug)]
pub(crate) struct UnionTagFunc;

impl IntrinsicFunction for UnionTagFunc {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.union_tag()
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "union_tag",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
}

#[derive(Debug)]
pub(crate) struct UnionExtractFunc;

impl IntrinsicFunction for UnionExtractFunc {
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.union_extract()
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "union_extract",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Unknown,
        }
    }
}
