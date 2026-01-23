//! Coverage instrumentation stubs (disabled mode).
//!
//! When the "enabled" feature is off, all functions return empty/default values
//! and the macro compiles to nothing.

use crate::common::{
    CoverageSnapshot, CoverageSummary, FileCoverage, FunctionCoverage, ModuleCoverage,
};

/// Track that this code path was reached.
///
/// When feature "enabled" is off, compiles to nothing (zero overhead).
#[macro_export]
macro_rules! coverage_point {
    ($name:literal) => {{}};
}

/// Get a report of all coverage points (returns empty when disabled).
pub fn get_coverage_report() -> Vec<CoverageSnapshot> {
    vec![]
}

/// Get the total number of instrumentation points (returns 0 when disabled).
pub fn total_points() -> usize {
    0
}

/// Get the number of points that have been hit (returns 0 when disabled).
pub fn covered_points() -> usize {
    0
}

/// Reset all coverage counters (no-op when disabled).
pub fn reset_coverage() {}

/// Get the coverage percentage (returns 100.0 when disabled).
pub fn coverage_percentage() -> f64 {
    100.0
}

/// Get only the points that have NOT been hit (returns empty when disabled).
pub fn uncovered_points() -> Vec<CoverageSnapshot> {
    vec![]
}

/// Get only the points that have been hit (returns empty when disabled).
pub fn covered_points_report() -> Vec<CoverageSnapshot> {
    vec![]
}

/// Get a summary of coverage statistics (returns zeros when disabled).
pub fn coverage_summary() -> CoverageSummary {
    CoverageSummary {
        total_points: 0,
        covered_points: 0,
        uncovered_points: 0,
        percentage: 100.0,
    }
}

/// Get coverage grouped by module path (returns empty when disabled).
pub fn coverage_by_module() -> Vec<ModuleCoverage> {
    vec![]
}

/// Get coverage grouped by source file (returns empty when disabled).
pub fn coverage_by_file() -> Vec<FileCoverage> {
    vec![]
}

/// Get coverage grouped by function (returns empty when disabled).
pub fn coverage_by_function() -> Vec<FunctionCoverage> {
    vec![]
}

/// Format a coverage report as a human-readable string.
pub fn format_coverage_report() -> String {
    String::from("Coverage instrumentation not enabled.\n")
}

/// Format a report of only uncovered points.
pub fn format_uncovered_report() -> String {
    String::from("Coverage instrumentation not enabled.\n")
}

/// Print the full coverage report to stdout.
pub fn print_coverage_report() {
    print!("{}", format_coverage_report());
}

/// Print only uncovered points to stdout.
pub fn print_uncovered_report() {
    print!("{}", format_uncovered_report());
}

/// Format a coverage report grouped by function.
pub fn format_function_coverage_report() -> String {
    String::from("Coverage instrumentation not enabled.\n")
}

/// Print the function coverage report to stdout.
pub fn print_function_coverage_report() {
    print!("{}", format_function_coverage_report());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stub_functions_compile() {
        // Verify all functions compile and return expected stub values
        assert_eq!(total_points(), 0);
        assert_eq!(covered_points(), 0);
        assert!((99.9..=100.1).contains(&coverage_percentage()));

        let report = get_coverage_report();
        assert!(report.is_empty());

        let uncovered = uncovered_points();
        assert!(uncovered.is_empty());

        let covered = covered_points_report();
        assert!(covered.is_empty());

        let summary = coverage_summary();
        assert_eq!(summary.total_points, 0);

        let by_module = coverage_by_module();
        assert!(by_module.is_empty());

        let by_file = coverage_by_file();
        assert!(by_file.is_empty());

        let by_function = coverage_by_function();
        assert!(by_function.is_empty());

        let report_str = format_coverage_report();
        assert!(report_str.contains("not enabled"));

        let uncovered_str = format_uncovered_report();
        assert!(uncovered_str.contains("not enabled"));

        // Verify macro compiles to nothing
        coverage_point!("this should be a no-op");

        // These should not panic
        reset_coverage();
        print_coverage_report();
        print_uncovered_report();
    }
}
