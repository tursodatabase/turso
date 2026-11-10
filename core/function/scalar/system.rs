use super::{
    FunctionArgs, FunctionArity, FunctionCompileContext, FunctionContext, ResultType, ReturnType,
    ScalarFunction, ScalarFunctionMetadata, ScalarState,
};
use crate::{IOResult, LimboError, Result, Value};

#[derive(Debug)]
pub(crate) struct TursoVersion;

impl ScalarFunction for TursoVersion {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "turso_version",
            arity: FunctionArity::Exact(0),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }

    fn execute(
        &self,
        context: &FunctionContext<'_>,
        _args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        if !context.connection.is_db_initialized() {
            return Ok(IOResult::Done(Value::build_text(
                crate::info::build::PKG_VERSION,
            )));
        }
        Ok(context
            .pager
            .with_header(|header| header.version_number)?
            .map(|version| Value::build_text(format_version(version.get() as i64))))
    }
}

#[cfg(all(feature = "fs", not(target_family = "wasm")))]
#[derive(Debug)]
pub(crate) struct LoadExtension;

#[cfg(all(feature = "fs", not(target_family = "wasm")))]
impl ScalarFunction for LoadExtension {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "load_extension",
            arity: FunctionArity::Variadic,
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Unknown,
        }
    }

    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        // Keep the existing arity errors and variadic function_list entry.
        if context.args.is_empty() {
            crate::bail_parse_error!("load_extension function with no arguments");
        }
        if context.args.len() != 1 {
            crate::bail_parse_error!("load_extension function called with not exactly 1 arguments");
        }
        context.compile_default()
    }

    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        use crate::security::permission::{Action, Object, PermissionDenied};
        if !context.connection.can_load_extensions() {
            crate::bail_parse_error!("runtime extension loading is disabled");
        }
        if !context.connection.current_role_is_superuser() {
            return Err(LimboError::PermissionDenied(Box::new(
                PermissionDenied::privilege(
                    Action::Execute,
                    Object::Function("load_extension".to_string()),
                ),
            )));
        }
        let ext = crate::connection::resolve_ext_path(&args.get(0).to_string())?;
        context.connection.load_extension(ext)?;
        Ok(IOResult::Done(Value::Null))
    }
}

#[derive(Debug)]
pub(crate) struct Attach;

impl ScalarFunction for Attach {
    fn is_internal(&self) -> bool {
        true
    }
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "attach",
            arity: FunctionArity::Exact(3),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Unknown,
        }
    }
    fn compile(&self, _context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        crate::bail_parse_error!("ATTACH should be handled at statement level, not as expression");
    }
    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        let Value::Text(filename) = args.get(0) else {
            return Err(LimboError::InvalidArgument(
                "attach: filename argument must be text".to_string(),
            ));
        };
        let Value::Text(dbname) = args.get(1) else {
            return Err(LimboError::InvalidArgument(
                "attach: database name argument must be text".to_string(),
            ));
        };
        let state = state.get_or_try_init(|| Ok(crate::attach::AttachDatabaseState::default()))?;
        // Attached sequence descriptors are loaded on the next schema reparse.
        // Preparing SQL here would drive I/O synchronously inside the VM.
        Ok(context
            .connection
            .attach_database(filename.as_str(), dbname.as_str(), state)?
            .map(|()| Value::Null))
    }
}

#[derive(Debug)]
pub(crate) struct Detach;

impl ScalarFunction for Detach {
    fn is_internal(&self) -> bool {
        true
    }
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "detach",
            arity: FunctionArity::Exact(1),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Unknown,
        }
    }
    fn compile(&self, _context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        crate::bail_parse_error!("DETACH should be handled at statement level, not as expression");
    }
    fn execute(
        &self,
        context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        let Value::Text(dbname) = args.get(0) else {
            return Err(LimboError::InvalidArgument(
                "detach: database name argument must be text".to_string(),
            ));
        };
        context.connection.detach_database(dbname.as_str())?;
        Ok(IOResult::Done(Value::Null))
    }
}

fn format_version(version: i64) -> String {
    let major = version / 1_000_000;
    let minor = (version % 1_000_000) / 1_000;
    let release = version % 1_000;
    format!("{major}.{minor}.{release}")
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_number_format() {
        assert_eq!(super::format_version(3046001), "3.46.1");
    }
}
