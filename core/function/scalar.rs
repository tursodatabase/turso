use crate::IOResult;
pub(crate) mod arrays;
pub(crate) mod comparison;
pub(crate) mod constructors;
pub(crate) mod context;
pub(crate) mod control;
pub(crate) mod datetime;
pub(crate) mod internal;
pub(crate) mod numeric;
pub(crate) mod pattern;
pub(crate) mod sequence;
mod state;
pub(crate) mod system;
pub(crate) mod values;
pub use state::ScalarState;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::LazyLock;

pub use super::arity::FunctionArity;
pub use super::{ResultType, ReturnType};
pub use crate::translate::expr::scalar::FunctionCompileContext;
use crate::vdbe::Register;
use crate::{Connection, Result, Value};

pub trait ScalarFunction: Debug + Send + Sync {
    fn metadata(&self) -> &ScalarFunctionMetadata;
    fn is_internal(&self) -> bool {
        false
    }
    fn compile(&self, context: FunctionCompileContext<'_, '_>) -> Result<usize> {
        context.compile_default()
    }
    fn argument_translation(&self) -> ArgumentTranslation {
        ArgumentTranslation::Values
    }

    fn explain_name(&self, _arg_count: usize) -> String {
        self.metadata().name.to_string()
    }

    fn date_time_arguments(&self) -> Option<DateTimeArguments> {
        None
    }
    /// Resume with the same arguments and state after I/O completes.
    /// The VM discards state on completion, error, reset, or cancellation.
    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        _args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Err(crate::LimboError::InternalError(format!(
            "{} must be compiled to dedicated instructions",
            self.metadata().name
        )))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgumentTranslation {
    Values,
    ComparedValues,
    ComparedPair,
}

/// Arguments checked for clock or timezone dependence in schema expressions.
#[derive(Debug, Clone, Copy)]
pub enum DateTimeArguments {
    TimeValue { index: usize },
    TimePair,
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
    connection: &'a crate::sync::Arc<Connection>,
    pager: &'a crate::storage::pager::Pager,
    clock: datetime::StatementClock<'a>,
    collation: crate::translate::collate::CollationSeq,
}

impl<'a> FunctionContext<'a> {
    pub fn new(
        connection: &'a crate::sync::Arc<Connection>,
        pager: &'a crate::storage::pager::Pager,
        current_time: &'a mut Option<crate::WallClockInstant>,
        collation: crate::translate::collate::CollationSeq,
    ) -> Self {
        Self {
            connection,
            pager,
            collation,
            clock: datetime::StatementClock::new(pager.io.as_ref(), current_time),
        }
    }

    pub fn compare(&self, lhs: &Value, rhs: &Value) -> std::cmp::Ordering {
        use crate::types::AsValueRef;
        self.collation
            .compare(lhs.as_value_ref(), rhs.as_value_ref())
    }

    pub fn clock(&self) -> &dyn crate::Clock {
        &self.clock
    }

    pub fn random_i64(&self) -> i64 {
        self.pager.io.generate_random_number()
    }

    pub fn fill_random_bytes(&self, dest: &mut [u8]) {
        self.pager.io.fill_bytes(dest);
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.connection.last_insert_rowid()
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
            self.by_name.insert(name, function);
        }
    }

    pub(crate) fn lookup(
        &self,
        normalized_name: &str,
        arg_count: usize,
    ) -> Option<&'static dyn ScalarFunction> {
        self.get(normalized_name)
            .filter(|function| function.metadata().arity.accepts(arg_count))
    }

    pub(crate) fn get(&self, normalized_name: &str) -> Option<&'static dyn ScalarFunction> {
        self.by_name.get(normalized_name).copied()
    }

    pub(crate) fn functions(&self) -> impl Iterator<Item = &'static dyn ScalarFunction> + '_ {
        self.by_name
            .iter()
            .filter(|(name, function)| **name == function.metadata().name)
            .map(|(_, function)| *function)
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
    catalog.register(&numeric::BooleanToInt, &[]);
    catalog.register(&numeric::IntToBoolean, &[]);
    catalog.register(&numeric::ValidateIpAddr, &[]);
    catalog.register(&numeric::NumericEncode, &[]);
    catalog.register(&numeric::NumericFit, &[]);
    catalog.register(&numeric::NumericDecode, &[]);
    catalog.register(&numeric::NumericNeg, &[]);
    catalog.register(&numeric::Gcd, &[]);
    catalog.register(&numeric::Lcm, &[]);
    catalog.register(&numeric::NumericAdd, &[]);
    catalog.register(&numeric::NumericSub, &[]);
    catalog.register(&numeric::NumericMul, &[]);
    catalog.register(&numeric::NumericDiv, &[]);
    catalog.register(&numeric::NumericMod, &[]);
    catalog.register(&context::Random, &[]);
    catalog.register(&context::RandomBlob, &[]);
    catalog.register(&context::LastInsertRowid, &[]);
    catalog.register(&context::SqliteVersion, &[]);
    catalog.register(&context::SqliteSourceId, &[]);
    catalog.register(&datetime::Date, &[]);
    catalog.register(&datetime::Time, &[]);
    catalog.register(&datetime::DateTime, &[]);
    catalog.register(&datetime::JulianDay, &[]);
    catalog.register(&datetime::UnixEpoch, &[]);
    catalog.register(&datetime::StrfTime, &[]);
    catalog.register(&datetime::TimeDiff, &[]);
    catalog.register(&datetime::DatePlus, &[]);
    catalog.register(&datetime::DateMinus, &[]);
    catalog.register(&datetime::DateDiff, &[]);
    catalog.register(&pattern::Glob, &[]);
    catalog.register(&pattern::Like, &[]);
    catalog.register(&comparison::Min, &[]);
    catalog.register(&comparison::Max, &[]);
    catalog.register(&comparison::Nullif, &[]);
    catalog.register(&control::Coalesce, &[]);
    catalog.register(&control::IfNull, &[]);
    catalog.register(&control::Iif, &["if"]);
    catalog.register(&control::Likely, &[]);
    catalog.register(&control::Unlikely, &[]);
    catalog.register(&control::Likelihood, &[]);
    catalog.register(&constructors::Array, &[]);
    catalog.register(&constructors::ArrayElement, &[]);
    catalog.register(&constructors::ArraySetElement, &[]);
    catalog.register(&constructors::StructPack, &[]);
    catalog.register(&constructors::StructExtractFunc, &[]);
    catalog.register(&constructors::UnionValueFunc, &[]);
    catalog.register(&constructors::UnionTagFunc, &[]);
    catalog.register(&constructors::UnionExtractFunc, &[]);
    catalog.register(&internal::ConnTxnId, &[]);
    catalog.register(&internal::IsAutocommit, &[]);
    catalog.register(&internal::TableColumnsJsonArray, &[]);
    catalog.register(&internal::BinRecordJsonObject, &[]);
    catalog.register(&internal::TestUintEncode, &[]);
    catalog.register(&internal::TestUintDecode, &[]);
    #[cfg(feature = "test_helper")]
    catalog.register(&internal::TestNondetCounter, &[]);
    catalog.register(&sequence::NextVal, &[]);
    catalog.register(&sequence::CurrVal, &[]);
    catalog.register(&sequence::SetVal, &[]);
    catalog.register(&sequence::SequenceWatermark, &[]);
    catalog.register(&system::TursoVersion, &[]);
    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    catalog.register(&system::LoadExtension, &[]);
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        args.get(0).exec_abs().map(IOResult::Done)
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(
            args.get(0).exec_lower().unwrap_or(Value::Null),
        ))
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(args.get(0).exec_length()))
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(args.get(0).exec_round(args.get_optional(1))))
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(args.get(0).exec_trim(args.get_optional(1))))
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(args.get(0).exec_ltrim(args.get_optional(1))))
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(args.get(0).exec_rtrim(args.get_optional(1))))
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(Value::exec_concat_strings(args.iter())))
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

    fn execute(
        &self,
        context: &FunctionContext<'_>,
        _args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(Value::from_i64(context.changes())))
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

    fn execute(
        &self,
        context: &FunctionContext<'_>,
        _args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(Value::from_i64(context.total_changes())))
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(
            args.get(0).exec_upper().unwrap_or(Value::Null),
        ))
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(args.get(0).exec_octet_length()))
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(args.get(0).exec_unicode()))
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(
            args.get(0).exec_sign().unwrap_or(Value::Null),
        ))
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(args.get(0).exec_soundex()))
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(args.get(0).exec_hex()))
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

    fn execute(
        &self,
        _context: &FunctionContext<'_>,
        args: FunctionArgs<'_>,
        _state: &mut ScalarState,
    ) -> Result<IOResult<Value>> {
        Ok(IOResult::Done(args.get(0).exec_unhex(args.get_optional(1))))
    }
}

#[cfg(test)]
mod tests {
    use crate::dialect::engine::{resolve_builtin_function, resolve_builtin_function_name};
    use crate::Func;

    #[test]
    fn catalog_registration_replaces_functions_and_aliases() {
        let mut catalog = super::FunctionCatalog::default();
        catalog.register(&super::Abs, &["absolute"]);
        catalog.register(&ReplacementAbs, &["absolute"]);
        for name in ["abs", "absolute"] {
            assert!(catalog.lookup(name, 1).is_none());
            assert!(matches!(
                catalog.lookup(name, 2).unwrap().metadata().arity,
                super::FunctionArity::Exact(2)
            ));
        }
        let functions: Vec<_> = catalog.functions().collect();
        assert_eq!(functions.len(), 1);
        assert!(matches!(
            functions[0].metadata().arity,
            super::FunctionArity::Exact(2)
        ));
    }

    #[test]
    fn catalog_lookup_uses_the_function_arity() {
        let mut catalog = super::FunctionCatalog::default();
        catalog.register(&super::Abs, &["absolute"]);
        for name in ["abs", "absolute"] {
            assert!(catalog.lookup(name, 0).is_none());
            assert!(catalog.lookup(name, 1).is_some());
            assert!(catalog.lookup(name, 2).is_none());
        }
    }

    #[derive(Debug)]
    struct ReplacementAbs;

    impl super::ScalarFunction for ReplacementAbs {
        fn metadata(&self) -> &super::ScalarFunctionMetadata {
            &super::ScalarFunctionMetadata {
                name: "abs",
                arity: super::FunctionArity::Exact(2),
                deterministic: true,
                can_mask_nulls: false,
                result_type: super::ResultType::Unknown,
            }
        }
    }

    #[test]
    fn scalar_and_aggregate_overloads_resolve_by_argument_count() {
        use crate::function::AggFunc;
        for (name, aggregate) in [("min", AggFunc::Min), ("max", AggFunc::Max)] {
            assert!(resolve_builtin_function(name, 0).is_err());
            assert!(
                matches!(resolve_builtin_function(name, 1).unwrap(), Some(Func::Agg(found)) if found == aggregate)
            );
            for count in [2, 3, 100] {
                let function = resolve_builtin_function(name, count).unwrap().unwrap();
                assert!(matches!(function, Func::Scalar(_)));
                assert!(function.needs_collation());
                assert_eq!(function.to_string(), name);
            }
        }
        assert!(matches!(
            resolve_builtin_function("nullif", 2).unwrap(),
            Some(Func::Scalar(_))
        ));
        for count in [0, 1, 3] {
            assert!(resolve_builtin_function_name("nullif", count)
                .unwrap()
                .is_some());
            assert!(resolve_builtin_function("nullif", count).is_err());
        }
        let functions = Func::builtin_function_list();
        for name in ["min", "max"] {
            assert_eq!(
                functions
                    .iter()
                    .filter(|f| f.name == name && f.func_type == "s" && f.narg == -1)
                    .count(),
                1
            );
            assert_eq!(
                functions
                    .iter()
                    .filter(|f| f.name == name && f.func_type == "w" && f.narg == 1)
                    .count(),
                1
            );
        }
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn argument_count_does_not_truncate() {
        let Func::Scalar(function) = resolve_builtin_function("abs", 1).unwrap().unwrap() else {
            panic!("expected a registered scalar");
        };
        assert!(function
            .metadata()
            .validate_arg_count("abs", (1usize << 32) + 1)
            .is_err());
        assert!(resolve_builtin_function("likelihood", (1usize << 32) + 2).is_err());
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
            assert!(matches!(function, Func::Scalar(_)));
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
                Some(Func::Scalar(_))
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
