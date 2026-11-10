use super::{
    FunctionArgs, FunctionArity, FunctionContext, ResultType, ReturnType, ScalarFunction,
    ScalarFunctionMetadata,
};
use crate::{Result, Value};

#[derive(Debug)]
pub(crate) struct Random;

impl ScalarFunction for Random {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "random",
            arity: FunctionArity::Exact(0),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(&self, context: &FunctionContext<'_>, _args: FunctionArgs<'_>) -> Result<Value> {
        Ok(Value::from_i64(context.random_i64()))
    }
}

#[derive(Debug)]
pub(crate) struct RandomBlob;

impl ScalarFunction for RandomBlob {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "randomblob",
            arity: FunctionArity::Exact(1),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Blob),
        }
    }
    fn execute(&self, context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        args.get(0)
            .exec_randomblob(|dest| context.fill_random_bytes(dest))
    }
}

#[derive(Debug)]
pub(crate) struct LastInsertRowid;

impl ScalarFunction for LastInsertRowid {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "last_insert_rowid",
            arity: FunctionArity::Exact(0),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }
    fn execute(&self, context: &FunctionContext<'_>, _args: FunctionArgs<'_>) -> Result<Value> {
        Ok(Value::from_i64(context.last_insert_rowid()))
    }
}

#[derive(Debug)]
pub(crate) struct SqliteVersion;

impl ScalarFunction for SqliteVersion {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "sqlite_version",
            arity: FunctionArity::Exact(0),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, _args: FunctionArgs<'_>) -> Result<Value> {
        Ok(Value::build_text(crate::dialect::engine::SQLITE_VERSION))
    }
}

#[derive(Debug)]
pub(crate) struct SqliteSourceId;

impl ScalarFunction for SqliteSourceId {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "sqlite_source_id",
            arity: FunctionArity::Exact(0),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }
    fn execute(&self, _context: &FunctionContext<'_>, _args: FunctionArgs<'_>) -> Result<Value> {
        Ok(Value::build_text(format!(
            "{} {}",
            crate::info::build::BUILT_TIME_SQLITE,
            crate::info::build::GIT_COMMIT_HASH.unwrap_or("unknown")
        )))
    }
}
