//! SQL function definitions for generation.
//!
//! This module defines the scalar functions that can be generated,
//! including their argument types, return types, and constraints.

use crate::context::Context;
use crate::schema::DataType;

/// Category of SQL function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionCategory {
    /// Mathematical functions (ABS, MAX, MIN, ROUND, etc.)
    Math,
    /// String manipulation functions (UPPER, LOWER, LENGTH, etc.)
    String,
    /// Type conversion and inspection (TYPEOF, ZEROBLOB, etc.)
    Type,
    /// Null handling functions (COALESCE, IFNULL, NULLIF)
    NullHandling,
    /// Control flow functions (IIF)
    ControlFlow,
    /// Date and time functions (DATE, TIME, DATETIME, etc.)
    DateTime,
    /// Blob functions (HEX, UNHEX, etc.)
    Blob,
    /// Aggregate functions (COUNT, SUM, AVG, etc.)
    Aggregate,
    /// Miscellaneous functions
    Other,
}

/// A SQL function definition.
#[derive(Debug, Clone)]
pub struct FunctionDef {
    /// The function name (e.g., "UPPER", "COALESCE").
    pub name: &'static str,
    /// The expected argument types. `None` means any type is accepted.
    /// For variadic functions, the last type is repeated.
    pub arg_types: &'static [Option<DataType>],
    /// Minimum number of arguments.
    pub min_args: usize,
    /// Maximum number of arguments.
    pub max_args: usize,
    /// The return type. `None` means the return type depends on arguments (first arg type).
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
            arg_types: &[],
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
    pub const fn args(mut self, types: &'static [Option<DataType>]) -> Self {
        self.arg_types = types;
        self.min_args = types.len();
        self.max_args = types.len();
        self
    }

    /// Set the arity for variadic functions.
    pub const fn arity(mut self, min: usize, max: usize) -> Self {
        self.min_args = min;
        self.max_args = max;
        self
    }

    /// Set the return type.
    pub const fn returns(mut self, data_type: DataType) -> Self {
        self.return_type = Some(data_type);
        self
    }

    /// Set the function category.
    pub const fn category(mut self, cat: FunctionCategory) -> Self {
        self.category = cat;
        self
    }

    /// Mark as an aggregate function.
    pub const fn aggregate(mut self) -> Self {
        self.is_aggregate = true;
        self
    }

    /// Mark as non-deterministic.
    pub const fn non_deterministic(mut self) -> Self {
        self.is_deterministic = false;
        self
    }

    /// Set maximum value for integer arguments (for functions that allocate memory).
    pub const fn int_arg_max(mut self, max: i64) -> Self {
        self.int_arg_max = Some(max);
        self
    }

    /// Get the number of arguments to generate for this function.
    pub fn arg_count(&self, ctx: &mut Context) -> usize {
        if self.min_args == self.max_args {
            self.min_args
        } else {
            ctx.gen_range_inclusive(self.min_args, self.max_args)
        }
    }

    /// Get the expected type for argument at the given position.
    /// Returns None if any type is acceptable.
    pub fn arg_type_at(&self, pos: usize) -> Option<DataType> {
        if self.arg_types.is_empty() {
            None
        } else if pos < self.arg_types.len() {
            self.arg_types[pos]
        } else if self.max_args > self.arg_types.len() {
            // Variadic: use last type
            *self.arg_types.last().unwrap_or(&None)
        } else {
            None
        }
    }

    /// Check if this function can accept a value of the given type at the given position.
    pub fn accepts_type_at(&self, pos: usize, data_type: DataType) -> bool {
        match self.arg_type_at(pos) {
            None => true, // Any type accepted
            Some(expected) => expected == data_type,
        }
    }
}

// Helper constants for argument type specifications
const ANY: Option<DataType> = None;
const TEXT: Option<DataType> = Some(DataType::Text);
const INT: Option<DataType> = Some(DataType::Integer);
const REAL: Option<DataType> = Some(DataType::Real);

/// All built-in scalar functions.
pub static SCALAR_FUNCTIONS: &[FunctionDef] = &[
    // =========================================================================
    // Math functions
    // =========================================================================
    FunctionDef::new("ABS")
        .args(&[ANY])
        .category(FunctionCategory::Math),
    FunctionDef::new("MAX")
        .args(&[ANY, ANY])
        .arity(2, 5)
        .category(FunctionCategory::Math),
    FunctionDef::new("MIN")
        .args(&[ANY, ANY])
        .arity(2, 5)
        .category(FunctionCategory::Math),
    FunctionDef::new("ROUND")
        .args(&[REAL])
        .returns(DataType::Real)
        .category(FunctionCategory::Math),
    FunctionDef::new("SIGN")
        .args(&[ANY])
        .returns(DataType::Integer)
        .category(FunctionCategory::Math),
    FunctionDef::new("RANDOM")
        .arity(0, 0)
        .returns(DataType::Integer)
        .category(FunctionCategory::Math)
        .non_deterministic(),
    // =========================================================================
    // String functions
    // =========================================================================
    FunctionDef::new("LENGTH")
        .args(&[TEXT])
        .returns(DataType::Integer)
        .category(FunctionCategory::String),
    FunctionDef::new("UPPER")
        .args(&[TEXT])
        .returns(DataType::Text)
        .category(FunctionCategory::String),
    FunctionDef::new("LOWER")
        .args(&[TEXT])
        .returns(DataType::Text)
        .category(FunctionCategory::String),
    FunctionDef::new("TRIM")
        .args(&[TEXT])
        .returns(DataType::Text)
        .category(FunctionCategory::String),
    FunctionDef::new("LTRIM")
        .args(&[TEXT])
        .returns(DataType::Text)
        .category(FunctionCategory::String),
    FunctionDef::new("RTRIM")
        .args(&[TEXT])
        .returns(DataType::Text)
        .category(FunctionCategory::String),
    FunctionDef::new("SUBSTR")
        .args(&[TEXT, INT, INT])
        .returns(DataType::Text)
        .category(FunctionCategory::String),
    FunctionDef::new("REPLACE")
        .args(&[TEXT, TEXT, TEXT])
        .returns(DataType::Text)
        .category(FunctionCategory::String),
    FunctionDef::new("INSTR")
        .args(&[TEXT, TEXT])
        .returns(DataType::Integer)
        .category(FunctionCategory::String),
    FunctionDef::new("PRINTF")
        .args(&[TEXT, ANY])
        .arity(1, 5)
        .returns(DataType::Text)
        .category(FunctionCategory::String),
    FunctionDef::new("UNICODE")
        .args(&[TEXT])
        .returns(DataType::Integer)
        .category(FunctionCategory::String),
    FunctionDef::new("CHAR")
        .args(&[INT])
        .arity(1, 5)
        .returns(DataType::Text)
        .category(FunctionCategory::String),
    FunctionDef::new("QUOTE")
        .args(&[ANY])
        .returns(DataType::Text)
        .category(FunctionCategory::String),
    // =========================================================================
    // Type functions
    // =========================================================================
    FunctionDef::new("TYPEOF")
        .args(&[ANY])
        .returns(DataType::Text)
        .category(FunctionCategory::Type),
    FunctionDef::new("ZEROBLOB")
        .args(&[INT])
        .returns(DataType::Blob)
        .category(FunctionCategory::Type)
        .int_arg_max(1000),
    // =========================================================================
    // Null handling functions
    // =========================================================================
    FunctionDef::new("COALESCE")
        .args(&[ANY, ANY])
        .arity(2, 5)
        .category(FunctionCategory::NullHandling),
    FunctionDef::new("IFNULL")
        .args(&[ANY, ANY])
        .category(FunctionCategory::NullHandling),
    FunctionDef::new("NULLIF")
        .args(&[ANY, ANY])
        .category(FunctionCategory::NullHandling),
    // =========================================================================
    // Control flow functions
    // =========================================================================
    FunctionDef::new("IIF")
        .args(&[INT, ANY, ANY])
        .category(FunctionCategory::ControlFlow),
    // =========================================================================
    // Date/time functions
    // =========================================================================
    FunctionDef::new("DATE")
        .args(&[TEXT])
        .returns(DataType::Text)
        .category(FunctionCategory::DateTime),
    FunctionDef::new("TIME")
        .args(&[TEXT])
        .returns(DataType::Text)
        .category(FunctionCategory::DateTime),
    FunctionDef::new("DATETIME")
        .args(&[TEXT])
        .returns(DataType::Text)
        .category(FunctionCategory::DateTime),
    FunctionDef::new("JULIANDAY")
        .args(&[TEXT])
        .returns(DataType::Real)
        .category(FunctionCategory::DateTime),
    FunctionDef::new("STRFTIME")
        .args(&[TEXT, TEXT])
        .returns(DataType::Text)
        .category(FunctionCategory::DateTime),
    FunctionDef::new("UNIXEPOCH")
        .args(&[TEXT])
        .returns(DataType::Integer)
        .category(FunctionCategory::DateTime),
    // =========================================================================
    // Blob functions
    // =========================================================================
    FunctionDef::new("HEX")
        .args(&[ANY])
        .returns(DataType::Text)
        .category(FunctionCategory::Blob),
    FunctionDef::new("UNHEX")
        .args(&[TEXT])
        .returns(DataType::Blob)
        .category(FunctionCategory::Blob),
    FunctionDef::new("RANDOMBLOB")
        .args(&[INT])
        .returns(DataType::Blob)
        .category(FunctionCategory::Blob)
        .int_arg_max(1000)
        .non_deterministic(),
    // =========================================================================
    // Other functions
    // =========================================================================
    FunctionDef::new("LIKELY")
        .args(&[ANY])
        .category(FunctionCategory::Other),
    FunctionDef::new("UNLIKELY")
        .args(&[ANY])
        .category(FunctionCategory::Other),
];

/// All built-in aggregate functions.
pub static AGGREGATE_FUNCTIONS: &[FunctionDef] = &[
    FunctionDef::new("COUNT")
        .args(&[ANY])
        .returns(DataType::Integer)
        .category(FunctionCategory::Aggregate)
        .aggregate(),
    FunctionDef::new("SUM")
        .args(&[ANY])
        .category(FunctionCategory::Aggregate)
        .aggregate(),
    FunctionDef::new("AVG")
        .args(&[ANY])
        .returns(DataType::Real)
        .category(FunctionCategory::Aggregate)
        .aggregate(),
    FunctionDef::new("TOTAL")
        .args(&[ANY])
        .returns(DataType::Real)
        .category(FunctionCategory::Aggregate)
        .aggregate(),
    FunctionDef::new("GROUP_CONCAT")
        .args(&[TEXT])
        .arity(1, 2)
        .returns(DataType::Text)
        .category(FunctionCategory::Aggregate)
        .aggregate(),
];

/// Get all scalar functions (non-aggregate, non-window).
pub fn scalar_functions() -> impl Iterator<Item = &'static FunctionDef> {
    SCALAR_FUNCTIONS
        .iter()
        .filter(|f| !f.is_aggregate && !f.is_window)
}

/// Get scalar functions by category.
pub fn functions_by_category(
    category: FunctionCategory,
) -> impl Iterator<Item = &'static FunctionDef> {
    scalar_functions().filter(move |f| f.category == category)
}

/// Get only deterministic functions.
pub fn deterministic_functions() -> impl Iterator<Item = &'static FunctionDef> {
    scalar_functions().filter(|f| f.is_deterministic)
}

/// Get aggregate functions.
pub fn aggregate_functions() -> impl Iterator<Item = &'static FunctionDef> {
    AGGREGATE_FUNCTIONS.iter()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_def_new() {
        let func = FunctionDef::new("ABS");
        assert_eq!(func.name, "ABS");
        assert!(func.is_deterministic);
        assert!(!func.is_aggregate);
    }

    #[test]
    fn test_function_def_with_args() {
        let func = FunctionDef::new("SUBSTR")
            .args(&[TEXT, INT, INT])
            .returns(DataType::Text);

        assert_eq!(func.name, "SUBSTR");
        assert_eq!(func.min_args, 3);
        assert_eq!(func.max_args, 3);
        assert_eq!(func.return_type, Some(DataType::Text));
    }

    #[test]
    fn test_variadic_function() {
        let func = FunctionDef::new("COALESCE").args(&[ANY, ANY]).arity(2, 5);

        assert_eq!(func.min_args, 2);
        assert_eq!(func.max_args, 5);
        // Any type accepted at any position
        assert!(func.accepts_type_at(0, DataType::Text));
        assert!(func.accepts_type_at(3, DataType::Integer));
    }

    #[test]
    fn test_arg_type_at() {
        let func = FunctionDef::new("SUBSTR").args(&[TEXT, INT, INT]);

        assert_eq!(func.arg_type_at(0), Some(DataType::Text));
        assert_eq!(func.arg_type_at(1), Some(DataType::Integer));
        assert_eq!(func.arg_type_at(2), Some(DataType::Integer));
    }

    #[test]
    fn test_scalar_functions_count() {
        let count = scalar_functions().count();
        assert!(count > 0, "Should have at least some scalar functions");
    }

    #[test]
    fn test_functions_by_category() {
        let math_funcs: Vec<_> = functions_by_category(FunctionCategory::Math).collect();
        assert!(math_funcs.iter().any(|f| f.name == "ABS"));
        assert!(math_funcs.iter().any(|f| f.name == "MAX"));
    }

    #[test]
    fn test_deterministic_functions() {
        let det_funcs: Vec<_> = deterministic_functions().collect();
        // RANDOMBLOB is non-deterministic
        assert!(!det_funcs.iter().any(|f| f.name == "RANDOMBLOB"));
        // ABS is deterministic
        assert!(det_funcs.iter().any(|f| f.name == "ABS"));
    }

    #[test]
    fn test_aggregate_functions() {
        let agg_funcs: Vec<_> = aggregate_functions().collect();
        assert!(agg_funcs.iter().any(|f| f.name == "COUNT"));
        assert!(agg_funcs.iter().any(|f| f.name == "SUM"));
        assert!(agg_funcs.iter().all(|f| f.is_aggregate));
    }

    #[test]
    fn test_function_names_are_uppercase() {
        for func in SCALAR_FUNCTIONS {
            assert_eq!(
                func.name,
                func.name.to_uppercase(),
                "Function {} should be uppercase",
                func.name
            );
        }
    }
}
