//! Common types shared between enabled and disabled modes.

use std::sync::LazyLock;
use std::sync::atomic::AtomicU64;

/// Captures the name of the enclosing function as a lazily initialized static.
///
/// This macro defines a nested function `_f()` within the caller's context,
/// then uses `type_name` to get its full path (e.g., `crate::module::function::_f`).
/// After trimming the `::_f` suffix, what remains is the enclosing function's path.
///
/// # Usage
///
/// ```ignore
/// fn my_function() {
///     function!(FUNC_NAME);
///     println!("In function: {}", *FUNC_NAME);
/// }
/// ```
#[macro_export]
macro_rules! function {
    ($static:ident) => {
        fn _f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            ::std::any::type_name::<T>()
        }
        static $static: ::std::sync::LazyLock<&'static str> = ::std::sync::LazyLock::new(|| {
            let name = type_name_of(_f);
            &name[..name.len() - 4]
        });
    };
}

/// Information about an instrumentation point.
pub struct CoveragePoint {
    /// User-provided name/description
    pub name: &'static str,
    /// Source file
    pub file: &'static str,
    /// Line number
    pub line: u32,
    /// Column number
    pub column: u32,
    /// Module path
    pub module: &'static str,
    /// Function name (initialized lazily)
    pub function: &'static LazyLock<&'static str>,
    /// How many times this point was hit
    pub hit_count: &'static AtomicU64,
}

// SAFETY: CoveragePoint only contains static references and atomics,
// which are both thread-safe.
unsafe impl Sync for CoveragePoint {}

/// A snapshot of a coverage point's state.
#[derive(Debug, Clone)]
pub struct CoverageSnapshot {
    /// User-provided name/description
    pub name: &'static str,
    /// Source file
    pub file: &'static str,
    /// Line number
    pub line: u32,
    /// Column number
    pub column: u32,
    /// Module path
    pub module: &'static str,
    /// Function name
    pub function: &'static str,
    /// How many times this point was hit
    pub hit_count: u64,
}

/// Summary statistics for coverage.
#[derive(Debug, Clone)]
pub struct CoverageSummary {
    pub total_points: usize,
    pub covered_points: usize,
    pub uncovered_points: usize,
    pub percentage: f64,
}

/// Coverage grouped by module.
#[derive(Debug, Clone)]
pub struct ModuleCoverage {
    pub module: &'static str,
    pub total: usize,
    pub covered: usize,
    pub percentage: f64,
    pub points: Vec<CoverageSnapshot>,
}

/// Coverage grouped by file.
#[derive(Debug, Clone)]
pub struct FileCoverage {
    pub file: &'static str,
    pub total: usize,
    pub covered: usize,
    pub percentage: f64,
    pub points: Vec<CoverageSnapshot>,
}

/// Coverage grouped by function.
#[derive(Debug, Clone)]
pub struct FunctionCoverage {
    pub function: &'static str,
    pub file: &'static str,
    pub total: usize,
    pub covered: usize,
    pub percentage: f64,
    pub points: Vec<CoverageSnapshot>,
}
