pub(crate) mod arrays;

pub(crate) mod values;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::LazyLock;

pub use super::arity::FunctionArity;
pub use super::{ResultType, ReturnType};
use crate::vdbe::Register;
use crate::{Connection, Result, Value};

pub trait ScalarFunction: Debug + Send + Sync {
    fn metadata(&self) -> &ScalarFunctionMetadata;
    fn execute(&self, context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value>;
}

#[derive(Debug)]
pub struct ScalarFunctionMetadata {
    pub name: &'static str,
    pub arity: FunctionArity,
    pub deterministic: bool,
    pub can_mask_nulls: bool,
    pub result_type: ResultType,
}

impl ScalarFunctionMetadata {
    pub fn validate_arg_count(&self, name: &str, arg_count: usize) -> Result<()> {
        if !self.arity.accepts(arg_count) {
            crate::bail_parse_error!("wrong number of arguments to function {}()", name);
        }
        Ok(())
    }
}

pub struct FunctionContext<'a> {
    connection: &'a Connection,
}

impl<'a> FunctionContext<'a> {
    pub fn new(connection: &'a Connection) -> Self {
        Self { connection }
    }

    pub fn changes(&self) -> i64 {
        self.connection.changes()
    }

    pub fn total_changes(&self) -> i64 {
        self.connection.total_changes()
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

    pub fn get_optional(&self, index: usize) -> Option<&'a Value> {
        self.registers.get(index).map(Register::get_value)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &'a Value> + 'a {
        self.registers.iter().map(Register::get_value)
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
        function
            .metadata()
            .arity
            .validate()
            .expect("invalid built-in function arity");
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
    catalog.register(&Round, &[]);
    catalog.register(&Trim, &["btrim"]);
    catalog.register(&LTrim, &[]);
    catalog.register(&RTrim, &[]);
    catalog.register(&Concat, &[]);
    catalog.register(&Changes, &[]);
    catalog.register(&TotalChanges, &[]);
    catalog.register(&Upper, &[]);
    catalog.register(&OctetLength, &[]);
    catalog.register(&Unicode, &[]);
    catalog.register(&Sign, &[]);
    catalog.register(&Soundex, &[]);
    catalog.register(&Hex, &[]);
    catalog.register(&Unhex, &[]);
    catalog.register(&values::Char, &["chr"]);
    catalog.register(&values::ConcatWs, &[]);
    catalog.register(&values::Instr, &["strpos"]);
    catalog.register(&values::Subtype, &[]);
    catalog.register(&values::Typeof, &[]);
    catalog.register(&values::Unistr, &[]);
    catalog.register(&values::UnistrQuote, &[]);
    catalog.register(&values::Quote, &[]);
    catalog.register(&values::ZeroBlob, &[]);
    catalog.register(&values::GetByte, &[]);
    catalog.register(&values::SetByte, &[]);
    catalog.register(&values::Replace, &[]);
    catalog.register(&values::Printf, &["format"]);
    catalog.register(&values::Substr, &[]);
    catalog.register(&values::Substring, &[]);
    catalog.register(&values::StringReverse, &["reverse"]);
    catalog.register(&values::Repeat, &[]);
    catalog.register(&values::Lpad, &[]);
    catalog.register(&values::Rpad, &[]);
    catalog.register(&arrays::ArrayLength, &["array_upper"]);
    catalog.register(&arrays::ArrayAppend, &[]);
    catalog.register(&arrays::ArrayCat, &[]);
    catalog.register(&arrays::ArrayRemove, &[]);
    catalog.register(&arrays::ArrayContains, &[]);
    catalog.register(&arrays::ArrayPosition, &[]);
    catalog.register(&arrays::ArrayOverlap, &["array_overlaps"]);
    catalog.register(&arrays::ArrayContainsAll, &[]);
    catalog.register(&arrays::ArrayPrepend, &[]);
    catalog.register(&arrays::ArraySlice, &[]);
    catalog.register(&arrays::StringToArray, &[]);
    catalog.register(&arrays::ArrayToString, &[]);
    catalog
});

#[derive(Debug)]
struct Abs;

impl ScalarFunction for Abs {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "abs",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::AbsOfTheFirstArgument,
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        args.get(0).exec_abs()
    }
}

#[derive(Debug)]
struct Lower;

impl ScalarFunction for Lower {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "lower",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_lower().unwrap_or(Value::Null))
    }
}

#[derive(Debug)]
struct Length;

impl ScalarFunction for Length {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "length",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_length())
    }
}

#[derive(Debug)]
struct Round;

impl ScalarFunction for Round {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "round",
            arity: FunctionArity::OneOf(&[1, 2]),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::RealForARealArgument,
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_round(args.get_optional(1)))
    }
}

#[derive(Debug)]
struct Trim;

impl ScalarFunction for Trim {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "trim",
            arity: FunctionArity::OneOf(&[1, 2]),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_trim(args.get_optional(1)))
    }
}

#[derive(Debug)]
struct LTrim;

impl ScalarFunction for LTrim {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "ltrim",
            arity: FunctionArity::OneOf(&[1, 2]),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_ltrim(args.get_optional(1)))
    }
}

#[derive(Debug)]
struct RTrim;

impl ScalarFunction for RTrim {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "rtrim",
            arity: FunctionArity::OneOf(&[1, 2]),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_rtrim(args.get_optional(1)))
    }
}

#[derive(Debug)]
struct Concat;

impl ScalarFunction for Concat {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "concat",
            arity: FunctionArity::AtLeast(1),
            deterministic: true,
            can_mask_nulls: true,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(Value::exec_concat_strings(args.iter()))
    }
}

#[derive(Debug)]
struct Changes;

impl ScalarFunction for Changes {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "changes",
            arity: FunctionArity::Exact(0),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }

    fn execute(&self, context: &FunctionContext<'_>, _args: FunctionArgs<'_>) -> Result<Value> {
        Ok(Value::from_i64(context.changes()))
    }
}

#[derive(Debug)]
struct TotalChanges;

impl ScalarFunction for TotalChanges {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "total_changes",
            arity: FunctionArity::Exact(0),
            deterministic: false,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }

    fn execute(&self, context: &FunctionContext<'_>, _args: FunctionArgs<'_>) -> Result<Value> {
        Ok(Value::from_i64(context.total_changes()))
    }
}

#[derive(Debug)]
struct Upper;

impl ScalarFunction for Upper {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "upper",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_upper().unwrap_or(Value::Null))
    }
}

#[derive(Debug)]
struct OctetLength;

impl ScalarFunction for OctetLength {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "octet_length",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_octet_length())
    }
}

#[derive(Debug)]
struct Unicode;

impl ScalarFunction for Unicode {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "unicode",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_unicode())
    }
}

#[derive(Debug)]
struct Sign;

impl ScalarFunction for Sign {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "sign",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Integer),
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_sign().unwrap_or(Value::Null))
    }
}

#[derive(Debug)]
struct Soundex;

impl ScalarFunction for Soundex {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "soundex",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: true,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_soundex())
    }
}

#[derive(Debug)]
struct Hex;

impl ScalarFunction for Hex {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "hex",
            arity: FunctionArity::Exact(1),
            deterministic: true,
            can_mask_nulls: true,
            result_type: ResultType::Declared(ReturnType::Text),
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_hex())
    }
}

#[derive(Debug)]
struct Unhex;

impl ScalarFunction for Unhex {
    fn metadata(&self) -> &ScalarFunctionMetadata {
        &ScalarFunctionMetadata {
            name: "unhex",
            arity: FunctionArity::OneOf(&[1, 2]),
            deterministic: true,
            can_mask_nulls: false,
            result_type: ResultType::Declared(ReturnType::Blob),
        }
    }

    fn execute(&self, _context: &FunctionContext<'_>, args: FunctionArgs<'_>) -> Result<Value> {
        Ok(args.get(0).exec_unhex(args.get_optional(1)))
    }
}

#[cfg(test)]
mod tests {
    use crate::dialect::engine::{resolve_builtin_function, resolve_builtin_function_name};
    use crate::Func;

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn argument_count_does_not_truncate() {
        let Func::RegisteredScalar(function) = resolve_builtin_function("abs", 1).unwrap().unwrap()
        else {
            panic!("expected a registered scalar");
        };
        assert!(function
            .metadata()
            .validate_arg_count("abs", (1usize << 32) + 1)
            .is_err());
        assert!(resolve_builtin_function("randomblob", (1usize << 32) + 1).is_err());
    }

    #[test]
    fn catalog_aliases_preserve_names_and_schema_loading() {
        for name in [
            "abs",
            "ABS",
            "lower",
            "LOWER",
            "upper",
            "octet_length",
            "unicode",
            "sign",
            "soundex",
            "hex",
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
        for count in [1, 2] {
            assert!(matches!(
                resolve_builtin_function("unhex", count).unwrap(),
                Some(Func::RegisteredScalar(_))
            ));
        }
        for count in [0, 3] {
            assert!(resolve_builtin_function("unhex", count).is_err());
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
