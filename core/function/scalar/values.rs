use super::{
    FunctionArgs, FunctionArity, FunctionContext, ResultType, ReturnType, ScalarFunction,
    ScalarFunctionMetadata,
};
use crate::{Result, Value};

#[derive(Debug)]
pub(crate) struct Char;

impl ScalarFunction for Char {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "char",
            arity: FunctionArity::Variadic,
            deterministic: true,
            can_mask_nulls: true,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(Value::exec_char(args.iter()))
    }
}

#[derive(Debug)]
pub(crate) struct ConcatWs;

impl ScalarFunction for ConcatWs {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "concat_ws",
            arity: FunctionArity::AtLeast(2),
            deterministic: true,
            can_mask_nulls: true,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(Value::exec_concat_ws(args.iter()))
    }
}

#[derive(Debug)]
pub(crate) struct Instr;

impl ScalarFunction for Instr {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "instr",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_instr(args.get(1)))
    }
}

#[derive(Debug)]
pub(crate) struct Subtype;

impl ScalarFunction for Subtype {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "subtype",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: true,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_subtype())
    }
}

#[derive(Debug)]
pub(crate) struct Typeof;

impl ScalarFunction for Typeof {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "typeof",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: true,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_typeof())
    }
}

#[derive(Debug)]
pub(crate) struct Unistr;

impl ScalarFunction for Unistr {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "unistr",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_unistr()?)
    }
}

#[derive(Debug)]
pub(crate) struct UnistrQuote;

impl ScalarFunction for UnistrQuote {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "unistr_quote",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: true,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_unistr_quote())
    }
}

#[derive(Debug)]
pub(crate) struct Quote;

impl ScalarFunction for Quote {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "quote",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: true,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_quote())
    }
}

#[derive(Debug)]
pub(crate) struct ZeroBlob;

impl ScalarFunction for ZeroBlob {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "zeroblob",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: true,
            result_type: ResultType::Declared(ReturnType::Blob),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_zeroblob()?)
    }
}

#[derive(Debug)]
pub(crate) struct GetByte;

impl ScalarFunction for GetByte {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "get_byte",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        args.get(0).exec_get_byte(args.get(1))
    }
}

#[derive(Debug)]
pub(crate) struct SetByte;

impl ScalarFunction for SetByte {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "set_byte",
            arity: FunctionArity::Exact(3),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Blob),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        args.get(0).exec_set_byte(args.get(1), args.get(2))
    }
}

#[derive(Debug)]
pub(crate) struct Replace;

impl ScalarFunction for Replace {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "replace",
            arity: FunctionArity::Exact(3),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(Value::exec_replace(args.get(0), args.get(1), args.get(2))?)
    }
}

#[derive(Debug)]
pub(crate) struct Printf;

impl ScalarFunction for Printf {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "printf",
            arity: FunctionArity::Variadic,
            deterministic: true,
            can_mask_nulls: true,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        crate::functions::printf::exec_printf(args.registers)
    }
}

#[derive(Debug)]
pub(crate) struct Substr;

impl ScalarFunction for Substr {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "substr",
            arity: FunctionArity::OneOf(&[2, 3]),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::TextOrBlobLikeTheFirstArgument,
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(Value::exec_substring(
            args.get(0),
            args.get(1),
            args.get_optional(2),
        )?)
    }
}

#[derive(Debug)]
pub(crate) struct Substring;

impl ScalarFunction for Substring {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "substring",
            arity: FunctionArity::OneOf(&[2, 3]),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::TextOrBlobLikeTheFirstArgument,
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(Value::exec_substring(
            args.get(0),
            args.get(1),
            args.get_optional(2),
        )?)
    }
}

#[derive(Debug)]
pub(crate) struct StringReverse;

impl ScalarFunction for StringReverse {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "string_reverse",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(match args.get(0) {
            Value::Null => Value::Null,
            value => Value::build_text(value.to_string().chars().rev().collect::<String>()),
        })
    }
}

#[derive(Debug)]
pub(crate) struct Repeat;

impl ScalarFunction for Repeat {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "repeat",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        crate::functions::string::exec_repeat(args.get(0), args.get(1))
    }
}

#[derive(Debug)]
pub(crate) struct Lpad;

impl ScalarFunction for Lpad {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "lpad",
            arity: FunctionArity::OneOf(&[2, 3]),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        crate::functions::string::exec_lpad(args.get(0), args.get(1), args.get_optional(2))
    }
}

#[derive(Debug)]
pub(crate) struct Rpad;

impl ScalarFunction for Rpad {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "rpad",
            arity: FunctionArity::OneOf(&[2, 3]),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        crate::functions::string::exec_rpad(args.get(0), args.get(1), args.get_optional(2))
    }
}
