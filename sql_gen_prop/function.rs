//! SQL function definitions and generation strategies.
//!
//! This module provides:
//! - `FunctionDef` - A definition of a SQL function with its signature
//! - `FunctionCategory` - Categories of functions (string, math, aggregate, etc.)
//! - `FunctionRegistry` - A registry of available functions for generation
//! - Built-in function definitions for SQLite-compatible functions
//!
//! # Adding New Functions
//!
//! To add a new function, use `FunctionDef::new()`:
//!
//! ```ignore
//! let my_func = FunctionDef::new("MY_FUNC")
//!     .args(&[DataType::Text, DataType::Integer])
//!     .returns(DataType::Text)
//!     .category(FunctionCategory::String);
//! ```

use std::fmt;

use proptest::prelude::*;
use strum::IntoEnumIterator;

use crate::generator::SqlGeneratorKind;
use crate::schema::DataType;

/// Categories of SQL functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum FunctionCategory {
    /// String manipulation functions (UPPER, LOWER, SUBSTR, etc.).
    String,
    /// Mathematical functions (ABS, ROUND, etc.).
    Math,
    /// Aggregate functions (COUNT, SUM, AVG, etc.).
    Aggregate,
    /// Date/time functions (DATE, TIME, DATETIME, etc.).
    DateTime,
    /// Type conversion and inspection (TYPEOF, CAST, etc.).
    Type,
    /// NULL handling functions (COALESCE, IFNULL, NULLIF, etc.).
    NullHandling,
    /// Control flow functions (IIF, CASE, etc.).
    ControlFlow,
    /// Blob/binary functions (HEX, UNHEX, etc.).
    Blob,
    /// JSON functions (JSON, JSON_EXTRACT, etc.).
    Json,
    /// Window functions (ROW_NUMBER, RANK, etc.).
    Window,
    /// Miscellaneous functions.
    Other,
}

impl fmt::Display for FunctionCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionCategory::String => write!(f, "String"),
            FunctionCategory::Math => write!(f, "Math"),
            FunctionCategory::Aggregate => write!(f, "Aggregate"),
            FunctionCategory::DateTime => write!(f, "DateTime"),
            FunctionCategory::Type => write!(f, "Type"),
            FunctionCategory::NullHandling => write!(f, "NullHandling"),
            FunctionCategory::ControlFlow => write!(f, "ControlFlow"),
            FunctionCategory::Blob => write!(f, "Blob"),
            FunctionCategory::Json => write!(f, "Json"),
            FunctionCategory::Window => write!(f, "Window"),
            FunctionCategory::Other => write!(f, "Other"),
        }
    }
}

/// Arity specification for functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arity {
    /// Fixed number of arguments.
    Fixed(usize),
    /// Variable number of arguments with min and max.
    Range { min: usize, max: usize },
    /// At least N arguments (variadic).
    AtLeast(usize),
}

impl Arity {
    /// Returns the minimum number of arguments.
    pub fn min(&self) -> usize {
        match self {
            Arity::Fixed(n) => *n,
            Arity::Range { min, .. } => *min,
            Arity::AtLeast(n) => *n,
        }
    }

    /// Returns the maximum number of arguments (capped at 10 for variadic).
    pub fn max(&self) -> usize {
        match self {
            Arity::Fixed(n) => *n,
            Arity::Range { max, .. } => *max,
            Arity::AtLeast(n) => n + 5, // Cap variadic at base + 5
        }
    }
}

/// A SQL function definition.
#[derive(Debug, Clone)]
pub struct FunctionDef {
    /// The function name (e.g., "UPPER", "COALESCE").
    pub name: &'static str,
    /// The expected argument types. `None` means any type is accepted.
    /// For variadic functions, the last type is repeated.
    pub arg_types: Vec<Option<DataType>>,
    /// Minimum number of arguments.
    pub min_args: usize,
    /// Maximum number of arguments.
    pub max_args: usize,
    /// The return type. `None` means the return type depends on arguments.
    pub return_type: Option<DataType>,
    /// The function category.
    pub category: FunctionCategory,
    /// Whether this is an aggregate function.
    pub is_aggregate: bool,
    /// Whether this is a window function.
    pub is_window: bool,
    /// Whether this function is deterministic (same inputs always give same output).
    pub is_deterministic: bool,
    /// Maximum value for integer arguments (for functions like ZEROBLOB that allocate memory).
    pub int_arg_max: Option<i64>,
}

impl FunctionDef {
    /// Create a new function definition with the given name.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            arg_types: Vec::new(),
            min_args: 0,
            max_args: 0,
            return_type: None,
            category: FunctionCategory::Other,
            is_aggregate: false,
            is_window: false,
            is_deterministic: true,
            int_arg_max: None,
        }
    }

    /// Set the argument types (creates a fixed-arity function).
    pub fn args(mut self, types: &[Option<DataType>]) -> Self {
        self.arg_types = types.to_vec();
        self.min_args = types.len();
        self.max_args = types.len();
        self
    }

    /// Set typed arguments using DataType directly.
    pub fn typed_args(mut self, types: &[DataType]) -> Self {
        self.arg_types = types.iter().map(|t| Some(*t)).collect();
        self.min_args = types.len();
        self.max_args = types.len();
        self
    }

    /// Set the arity for variadic functions.
    pub fn arity(mut self, min: usize, max: usize) -> Self {
        self.min_args = min;
        self.max_args = max;
        self
    }

    /// Set the return type.
    pub fn returns(mut self, data_type: DataType) -> Self {
        self.return_type = Some(data_type);
        self
    }

    /// Set the return type to match the first argument.
    pub fn returns_first_arg_type(mut self) -> Self {
        self.return_type = None;
        self
    }

    /// Set the function category.
    pub fn category(mut self, cat: FunctionCategory) -> Self {
        self.category = cat;
        self
    }

    /// Mark as an aggregate function.
    pub fn aggregate(mut self) -> Self {
        self.is_aggregate = true;
        self.category = FunctionCategory::Aggregate;
        self
    }

    /// Mark as a window function.
    pub fn window(mut self) -> Self {
        self.is_window = true;
        self.category = FunctionCategory::Window;
        self
    }

    /// Mark as non-deterministic.
    pub fn non_deterministic(mut self) -> Self {
        self.is_deterministic = false;
        self
    }

    /// Set maximum value for integer arguments (for functions that allocate memory).
    pub fn int_arg_max(mut self, max: i64) -> Self {
        self.int_arg_max = Some(max);
        self
    }

    /// Check if this function can accept a value of the given type at the given position.
    pub fn accepts_type_at(&self, pos: usize, data_type: &DataType) -> bool {
        if pos >= self.arg_types.len() {
            // Beyond defined args - check if variadic
            if self.max_args > self.arg_types.len() && !self.arg_types.is_empty() {
                // Variadic: use last type
                self.arg_types
                    .last()
                    .map(|t| t.as_ref().is_none() || t.as_ref() == Some(data_type))
                    .unwrap_or(true)
            } else {
                false
            }
        } else {
            self.arg_types[pos].as_ref().is_none()
                || self.arg_types[pos].as_ref() == Some(data_type)
        }
    }

    /// Get the expected type for argument at the given position.
    pub fn expected_type_at(&self, pos: usize) -> Option<&DataType> {
        if pos < self.arg_types.len() {
            self.arg_types[pos].as_ref()
        } else if !self.arg_types.is_empty() && self.max_args > self.arg_types.len() {
            // Variadic: use last type
            self.arg_types.last().and_then(|t| t.as_ref())
        } else {
            None
        }
    }
}

/// Profile for controlling function generation weights.
#[derive(Debug, Clone)]
pub struct FunctionProfile {
    pub string_weight: u32,
    pub math_weight: u32,
    pub aggregate_weight: u32,
    pub datetime_weight: u32,
    pub type_weight: u32,
    pub null_handling_weight: u32,
    pub control_flow_weight: u32,
    pub blob_weight: u32,
    pub json_weight: u32,
    pub window_weight: u32,
    pub other_weight: u32,
}

impl Default for FunctionProfile {
    fn default() -> Self {
        Self {
            string_weight: 10,
            math_weight: 10,
            aggregate_weight: 5,
            datetime_weight: 5,
            type_weight: 5,
            null_handling_weight: 8,
            control_flow_weight: 5,
            blob_weight: 3,
            json_weight: 3,
            window_weight: 2,
            other_weight: 5,
        }
    }
}

impl FunctionProfile {
    /// Get the weight for a function category.
    pub fn weight_for(&self, category: FunctionCategory) -> u32 {
        match category {
            FunctionCategory::String => self.string_weight,
            FunctionCategory::Math => self.math_weight,
            FunctionCategory::Aggregate => self.aggregate_weight,
            FunctionCategory::DateTime => self.datetime_weight,
            FunctionCategory::Type => self.type_weight,
            FunctionCategory::NullHandling => self.null_handling_weight,
            FunctionCategory::ControlFlow => self.control_flow_weight,
            FunctionCategory::Blob => self.blob_weight,
            FunctionCategory::Json => self.json_weight,
            FunctionCategory::Window => self.window_weight,
            FunctionCategory::Other => self.other_weight,
        }
    }

    pub fn enabled_operations(&self) -> impl Iterator<Item = (FunctionCategory, u32)> {
        FunctionCategory::iter()
            .map(|cat| (cat, self.weight_for(cat)))
            .filter(|(_, w)| *w > 0)
    }
}

/// Context for function generation.
#[derive(Debug, Clone)]
pub struct FunctionContext<'a> {
    /// Available columns for column references in arguments.
    pub columns: Option<&'a [crate::schema::ColumnDef]>,
    /// Whether aggregate functions are allowed.
    pub allow_aggregates: bool,
    /// Whether window functions are allowed.
    pub allow_windows: bool,
}

impl<'a> FunctionContext<'a> {
    pub fn new() -> Self {
        Self {
            columns: None,
            allow_aggregates: false,
            allow_windows: false,
        }
    }
}

impl Default for FunctionContext<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlGeneratorKind for FunctionCategory {
    type Context<'a> = FunctionContext<'a>;
    type Output = FunctionDef;
    type Profile = FunctionProfile;

    fn available(&self, ctx: &Self::Context<'_>) -> bool {
        match self {
            FunctionCategory::Aggregate => ctx.allow_aggregates,
            FunctionCategory::Window => ctx.allow_windows,
            _ => true,
        }
    }

    fn strategy<'a>(
        &self,
        _ctx: &Self::Context<'a>,
        _profile: Option<&Self::Profile>,
    ) -> BoxedStrategy<Self::Output> {
        let funcs = functions_in_category(*self);
        if funcs.is_empty() {
            // Return a dummy that won't be used (filtered out by available())
            Just(FunctionDef::new("_INVALID")).boxed()
        } else {
            proptest::sample::select(funcs).boxed()
        }
    }
}

/// A registry of available SQL functions.
#[derive(Debug, Clone, Default)]
pub struct FunctionRegistry {
    functions: Vec<FunctionDef>,
}

impl FunctionRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            functions: Vec::new(),
        }
    }

    /// Add a function to the registry.
    pub fn register(mut self, func: FunctionDef) -> Self {
        self.functions.push(func);
        self
    }

    /// Add multiple functions to the registry.
    pub fn register_all(mut self, funcs: impl IntoIterator<Item = FunctionDef>) -> Self {
        self.functions.extend(funcs);
        self
    }

    /// Get all functions in the registry.
    pub fn all(&self) -> &[FunctionDef] {
        &self.functions
    }

    /// Get functions in a specific category.
    pub fn in_category(&self, category: FunctionCategory) -> impl Iterator<Item = &FunctionDef> {
        self.functions
            .iter()
            .filter(move |f| f.category == category)
    }

    /// Get functions that return a specific type.
    pub fn functions_returning(
        &self,
        data_type: Option<&DataType>,
    ) -> impl Iterator<Item = &FunctionDef> {
        self.functions
            .iter()
            .filter(move |f| match (data_type, &f.return_type) {
                (None, _) => true,
                (Some(_), None) => true, // Function returns argument type
                (Some(t), Some(rt)) => t == rt,
            })
    }

    /// Get non-aggregate functions.
    pub fn scalar_functions(&self) -> impl Iterator<Item = &FunctionDef> {
        self.functions.iter().filter(|f| !f.is_aggregate)
    }

    /// Get aggregate functions.
    pub fn aggregate_functions(&self) -> impl Iterator<Item = &FunctionDef> {
        self.functions.iter().filter(|f| f.is_aggregate)
    }

    /// Get window functions.
    pub fn window_functions(&self) -> impl Iterator<Item = &FunctionDef> {
        self.functions.iter().filter(|f| f.is_window)
    }
}

/// Get functions in a specific category (from built-ins).
pub fn functions_in_category(category: FunctionCategory) -> Vec<FunctionDef> {
    builtin_functions()
        .functions
        .into_iter()
        .filter(|f| f.category == category)
        .collect()
}

/// Create a registry with all built-in SQLite functions.
pub fn builtin_functions() -> FunctionRegistry {
    FunctionRegistry::new()
        .register_all(string_functions())
        .register_all(math_functions())
        .register_all(aggregate_functions())
        .register_all(datetime_functions())
        .register_all(type_functions())
        .register_all(null_handling_functions())
        .register_all(control_flow_functions())
        .register_all(blob_functions())
}

/// Built-in string functions.
pub fn string_functions() -> Vec<FunctionDef> {
    vec![
        FunctionDef::new("LENGTH")
            .typed_args(&[DataType::Text])
            .returns(DataType::Integer)
            .category(FunctionCategory::String),
        FunctionDef::new("UPPER")
            .typed_args(&[DataType::Text])
            .returns(DataType::Text)
            .category(FunctionCategory::String),
        FunctionDef::new("LOWER")
            .typed_args(&[DataType::Text])
            .returns(DataType::Text)
            .category(FunctionCategory::String),
        FunctionDef::new("TRIM")
            .typed_args(&[DataType::Text])
            .returns(DataType::Text)
            .category(FunctionCategory::String),
        FunctionDef::new("LTRIM")
            .typed_args(&[DataType::Text])
            .returns(DataType::Text)
            .category(FunctionCategory::String),
        FunctionDef::new("RTRIM")
            .typed_args(&[DataType::Text])
            .returns(DataType::Text)
            .category(FunctionCategory::String),
        FunctionDef::new("SUBSTR")
            .typed_args(&[DataType::Text, DataType::Integer, DataType::Integer])
            .returns(DataType::Text)
            .category(FunctionCategory::String),
        FunctionDef::new("REPLACE")
            .typed_args(&[DataType::Text, DataType::Text, DataType::Text])
            .returns(DataType::Text)
            .category(FunctionCategory::String),
        FunctionDef::new("INSTR")
            .typed_args(&[DataType::Text, DataType::Text])
            .returns(DataType::Integer)
            .category(FunctionCategory::String),
        FunctionDef::new("PRINTF")
            .args(&[Some(DataType::Text), None])
            .arity(1, 10)
            .returns(DataType::Text)
            .category(FunctionCategory::String),
        FunctionDef::new("UNICODE")
            .typed_args(&[DataType::Text])
            .returns(DataType::Integer)
            .category(FunctionCategory::String),
        FunctionDef::new("CHAR")
            .args(&[Some(DataType::Integer)])
            .arity(1, 10)
            .returns(DataType::Text)
            .category(FunctionCategory::String),
        FunctionDef::new("QUOTE")
            .args(&[None])
            .returns(DataType::Text)
            .category(FunctionCategory::String),
    ]
}

/// Built-in math functions.
pub fn math_functions() -> Vec<FunctionDef> {
    vec![
        FunctionDef::new("ABS")
            .args(&[None])
            .returns_first_arg_type()
            .category(FunctionCategory::Math),
        FunctionDef::new("ROUND")
            .typed_args(&[DataType::Real])
            .returns(DataType::Real)
            .category(FunctionCategory::Math),
        FunctionDef::new("MAX")
            .args(&[None, None])
            .arity(2, 10)
            .returns_first_arg_type()
            .category(FunctionCategory::Math),
        FunctionDef::new("MIN")
            .args(&[None, None])
            .arity(2, 10)
            .returns_first_arg_type()
            .category(FunctionCategory::Math),
        FunctionDef::new("RANDOM")
            .arity(0, 0)
            .returns(DataType::Integer)
            .category(FunctionCategory::Math)
            .non_deterministic(),
        FunctionDef::new("SIGN")
            .args(&[None])
            .returns(DataType::Integer)
            .category(FunctionCategory::Math),
    ]
}

/// Built-in aggregate functions.
pub fn aggregate_functions() -> Vec<FunctionDef> {
    vec![
        FunctionDef::new("COUNT")
            .args(&[None])
            .returns(DataType::Integer)
            .aggregate(),
        FunctionDef::new("SUM")
            .args(&[None])
            .returns_first_arg_type()
            .aggregate(),
        FunctionDef::new("AVG")
            .args(&[None])
            .returns(DataType::Real)
            .aggregate(),
        FunctionDef::new("TOTAL")
            .args(&[None])
            .returns(DataType::Real)
            .aggregate(),
        FunctionDef::new("GROUP_CONCAT")
            .args(&[Some(DataType::Text)])
            .arity(1, 2)
            .returns(DataType::Text)
            .aggregate(),
    ]
}

/// Built-in date/time functions.
pub fn datetime_functions() -> Vec<FunctionDef> {
    vec![
        FunctionDef::new("DATE")
            .typed_args(&[DataType::Text])
            .returns(DataType::Text)
            .category(FunctionCategory::DateTime),
        FunctionDef::new("TIME")
            .typed_args(&[DataType::Text])
            .returns(DataType::Text)
            .category(FunctionCategory::DateTime),
        FunctionDef::new("DATETIME")
            .typed_args(&[DataType::Text])
            .returns(DataType::Text)
            .category(FunctionCategory::DateTime),
        FunctionDef::new("JULIANDAY")
            .typed_args(&[DataType::Text])
            .returns(DataType::Real)
            .category(FunctionCategory::DateTime),
        FunctionDef::new("STRFTIME")
            .typed_args(&[DataType::Text, DataType::Text])
            .returns(DataType::Text)
            .category(FunctionCategory::DateTime),
        FunctionDef::new("UNIXEPOCH")
            .typed_args(&[DataType::Text])
            .returns(DataType::Integer)
            .category(FunctionCategory::DateTime),
    ]
}

/// Built-in type functions.
pub fn type_functions() -> Vec<FunctionDef> {
    vec![
        FunctionDef::new("TYPEOF")
            .args(&[None])
            .returns(DataType::Text)
            .category(FunctionCategory::Type),
        FunctionDef::new("ZEROBLOB")
            .typed_args(&[DataType::Integer])
            .returns(DataType::Blob)
            .category(FunctionCategory::Type)
            .int_arg_max(1024),
    ]
}

/// Built-in NULL handling functions.
pub fn null_handling_functions() -> Vec<FunctionDef> {
    vec![
        FunctionDef::new("COALESCE")
            .args(&[None, None])
            .arity(2, 10)
            .returns_first_arg_type()
            .category(FunctionCategory::NullHandling),
        FunctionDef::new("IFNULL")
            .args(&[None, None])
            .returns_first_arg_type()
            .category(FunctionCategory::NullHandling),
        FunctionDef::new("NULLIF")
            .args(&[None, None])
            .returns_first_arg_type()
            .category(FunctionCategory::NullHandling),
    ]
}

/// Built-in control flow functions.
pub fn control_flow_functions() -> Vec<FunctionDef> {
    vec![
        FunctionDef::new("IIF")
            .args(&[Some(DataType::Integer), None, None])
            .returns_first_arg_type()
            .category(FunctionCategory::ControlFlow),
    ]
}

/// Built-in blob functions.
pub fn blob_functions() -> Vec<FunctionDef> {
    vec![
        FunctionDef::new("HEX")
            .args(&[None])
            .returns(DataType::Text)
            .category(FunctionCategory::Blob),
        FunctionDef::new("UNHEX")
            .typed_args(&[DataType::Text])
            .returns(DataType::Blob)
            .category(FunctionCategory::Blob),
    ]
}

/// Generate a strategy for selecting a function from the registry.
pub fn function_from_registry(registry: &FunctionRegistry) -> BoxedStrategy<FunctionDef> {
    let funcs = registry.functions.clone();
    if funcs.is_empty() {
        Just(FunctionDef::new("_EMPTY")).boxed()
    } else {
        proptest::sample::select(funcs).boxed()
    }
}

/// Generate a strategy for selecting a scalar (non-aggregate) function.
pub fn scalar_function(registry: &FunctionRegistry) -> BoxedStrategy<FunctionDef> {
    let funcs: Vec<FunctionDef> = registry.scalar_functions().cloned().collect();
    if funcs.is_empty() {
        Just(FunctionDef::new("_EMPTY")).boxed()
    } else {
        proptest::sample::select(funcs).boxed()
    }
}

/// Generate a strategy for selecting an aggregate function.
pub fn aggregate_function(registry: &FunctionRegistry) -> BoxedStrategy<FunctionDef> {
    let funcs: Vec<FunctionDef> = registry.aggregate_functions().cloned().collect();
    if funcs.is_empty() {
        Just(FunctionDef::new("_EMPTY")).boxed()
    } else {
        proptest::sample::select(funcs).boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_def_builder() {
        let func = FunctionDef::new("TEST")
            .typed_args(&[DataType::Text, DataType::Integer])
            .returns(DataType::Text)
            .category(FunctionCategory::String);

        assert_eq!(func.name, "TEST");
        assert_eq!(func.min_args, 2);
        assert_eq!(func.max_args, 2);
        assert_eq!(func.return_type, Some(DataType::Text));
        assert_eq!(func.category, FunctionCategory::String);
        assert!(!func.is_aggregate);
    }

    #[test]
    fn test_variadic_function() {
        let func = FunctionDef::new("COALESCE")
            .args(&[None, None])
            .arity(2, 10);

        assert_eq!(func.min_args, 2);
        assert_eq!(func.max_args, 10);
        assert!(func.accepts_type_at(0, &DataType::Text));
        assert!(func.accepts_type_at(5, &DataType::Integer));
    }

    #[test]
    fn test_registry() {
        let registry = builtin_functions();

        assert!(!registry.all().is_empty());
        assert!(registry.in_category(FunctionCategory::String).count() > 0);
        assert!(registry.scalar_functions().count() > 0);
        assert!(registry.aggregate_functions().count() > 0);
    }

    #[test]
    fn test_functions_returning_type() {
        let registry = builtin_functions();

        let mut text_funcs = registry.functions_returning(Some(&DataType::Text));
        assert!(text_funcs.any(|f| f.name == "UPPER"));

        let mut int_funcs = registry.functions_returning(Some(&DataType::Integer));
        assert!(int_funcs.any(|f| f.name == "LENGTH"));
    }

    proptest::proptest! {
        #[test]
        fn generated_function_is_valid(func in function_from_registry(&builtin_functions())) {
            proptest::prop_assert!(!func.name.is_empty());
            proptest::prop_assert!(func.min_args <= func.max_args);
        }
    }
}
