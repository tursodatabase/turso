use super::{
    FunctionArgs, FunctionArity, FunctionContext, ResultType, ReturnType, ScalarFunction,
    ScalarFunctionMetadata,
};
use crate::{Result, Value};

#[derive(Debug)]
pub(crate) struct Glob;

impl ScalarFunction for Glob {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "glob",
            arity: FunctionArity::Exact(2),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        if args.len() != 2 {
            return Err(crate::LimboError::ParseError(
                "wrong number of arguments to function GLOB()".to_string(),
            ));
        }
        let (pattern, text) = (args.get(0), args.get(1));
        if matches!(pattern, Value::Null) || matches!(text, Value::Null) {
            return Ok(Value::Null);
        }
        Ok(Value::from_i64(
            Value::exec_glob(&as_text(pattern)?, &as_text(text)?)? as i64,
        ))
    }
}

#[derive(Debug)]
pub(crate) struct Like;

impl ScalarFunction for Like {
    fn explain_name(&self, arg_count: usize) -> String {
        format!("like({arg_count})")
    }

    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "like",
            arity: FunctionArity::OneOf(&[2, 3]),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        let (pattern, text) = (args.get(0), args.get(1));
        if matches!(pattern, Value::Null) || matches!(text, Value::Null) {
            return Ok(Value::Null);
        }
        let escape = match args.get_optional(2) {
            None => None,
            Some(Value::Null) => return Ok(Value::Null),
            Some(value) => {
                let text = as_text(value)?;
                let mut chars = text.chars();
                let first = chars.next();
                if first.is_none() || chars.next().is_some() {
                    return Err(crate::LimboError::SqlError(
                        "ESCAPE expression must be a single character".to_string(),
                    ));
                }
                first
            }
        };
        let matches = match (pattern, text) {
            (Value::Text(pattern), Value::Text(text)) => {
                Value::exec_like(pattern.as_str(), text.as_str(), escape)?
            }
            _ => exec_like_converted(pattern, text, escape)?,
        };
        Ok(Value::from_i64(matches as i64))
    }
}

#[inline(never)]
fn exec_like_converted(pattern: &Value, text: &Value, escape: Option<char>) -> Result<bool> {
    Value::exec_like(&as_text(pattern)?, &as_text(text)?, escape)
}

fn as_text(value: &Value) -> Result<std::borrow::Cow<'_, str>> {
    Ok(match value {
        Value::Text(text) => std::borrow::Cow::Borrowed(text.as_str()),
        value => match value.exec_cast("TEXT")? {
            Value::Text(text) => std::borrow::Cow::Owned(text.to_string()),
            _ => unreachable!("Cast to TEXT should yield Text"),
        },
    })
}
