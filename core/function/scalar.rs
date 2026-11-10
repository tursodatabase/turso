use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::LazyLock;

pub use super::{ResultType, ReturnType};
use crate::vdbe::Register;
use crate::{Result, Value};

pub trait ScalarFunction: Debug + Send + Sync {
    fn metadata(&self) -> &ScalarFunctionMetadata;
    fn execute(&self, args: FunctionArgs<'_>) -> Result<Value>;
}

#[derive(Debug)]
pub struct ScalarFunctionMetadata {
    pub name: &'static str,
    pub arities: &'static [i32],
    pub deterministic: bool,
    pub can_mask_nulls: bool,
    pub result_type: ResultType,
}

impl ScalarFunctionMetadata {
    pub fn validate_arg_count(&self, name: &str, arg_count: usize) -> Result<()> {
        if !self.arities.contains(&-1) && !self.arities.contains(&(arg_count as i32)) {
            crate::bail_parse_error!("wrong number of arguments to function {}()", name);
        }
        Ok(())
    }
}

pub struct FunctionArgs<'a> {
    registers: &'a [Register],
}

impl<'a> FunctionArgs<'a> {
    pub fn new(registers: &'a [Register]) -> Self {
        Self { registers }
    }

    pub fn get(&self, index: usize) -> &'a Value {
        self.registers[index].get_value()
    }

    pub fn len(&self) -> usize {
        self.registers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.registers.is_empty()
    }
}

#[derive(Default)]
pub(crate) struct FunctionCatalog {
    by_name: HashMap<&'static str, &'static dyn ScalarFunction>,
    functions: Vec<&'static dyn ScalarFunction>,
}

impl FunctionCatalog {
    pub(crate) fn register(
        &mut self,
        function: &'static dyn ScalarFunction,
        aliases: &[&'static str],
    ) {
        for name in std::iter::once(function.metadata().name).chain(aliases.iter().copied()) {
            assert_eq!(name, crate::util::normalize_ident(name));
            assert!(
                self.by_name.insert(name, function).is_none(),
                "duplicate function: {name}"
            );
        }
        self.functions.push(function);
    }

    pub(crate) fn lookup(&self, normalized_name: &str) -> Option<&'static dyn ScalarFunction> {
        self.by_name.get(normalized_name).copied()
    }

    pub(crate) fn functions(&self) -> impl Iterator<Item = &'static dyn ScalarFunction> + '_ {
        self.functions.iter().copied()
    }
}

pub(crate) static BUILTIN_SCALAR_FUNCTIONS: LazyLock<FunctionCatalog> = LazyLock::new(|| {
    let mut catalog = FunctionCatalog::default();
    catalog.register(&Abs, &[]);
    catalog.register(&Lower, &[]);
    catalog.register(&Length, &["char_length", "character_length"]);
    catalog
});

#[derive(Debug)]
struct Abs;

impl ScalarFunction for Abs {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "abs",
            arities: &[1],
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::AbsOfTheFirstArgument,
        }
    }

    fn execute(&self, args: FunctionArgs<'_>) -> Result<Value> {
        args.get(0).exec_abs()
    }
}

#[derive(Debug)]
struct Lower;

impl ScalarFunction for Lower {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "lower",
            arities: &[1],
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }

    fn execute(&self, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_lower().unwrap_or(Value::Null))
    }
}

#[derive(Debug)]
struct Length;

impl ScalarFunction for Length {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "length",
            arities: &[1],
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }

    fn execute(&self, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_length())
    }
}

#[cfg(test)]
mod tests {
    use crate::dialect::engine::{resolve_builtin_function, resolve_builtin_function_name};
    use crate::Func;

    #[test]
    fn catalog_aliases_preserve_names_and_schema_loading() {
        for name in [
            "abs",
            "ABS",
            "lower",
            "LOWER",
            "length",
            "char_length",
            "CHARACTER_LENGTH",
        ] {
            let function = resolve_builtin_function(name, 1).unwrap().unwrap();
            assert!(matches!(function, Func::RegisteredScalar(_)));
            for count in [0, 2] {
                assert!(resolve_builtin_function(name, count).is_err());
                assert!(resolve_builtin_function_name(name, count)
                    .unwrap()
                    .is_some());
            }
        }
        for alias in ["length", "char_length", "character_length"] {
            assert_eq!(
                resolve_builtin_function(alias, 1)
                    .unwrap()
                    .unwrap()
                    .to_string(),
                "length"
            );
        }
    }
}
