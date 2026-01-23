//! Coverage instrumentation crate for tracking code path execution.
//!
//! This crate provides compile-time instrumentation points that track which code paths
//! were executed and how many times. When the "enabled" feature is off, all macros
//! compile to nothing (zero overhead).
//!
//! # Usage
//!
//! ```ignore
//! use turso_coverage::coverage_point;
//!
//! fn process_data(data: &[u8]) {
//!     coverage_point!("process_data entry");
//!
//!     if data.is_empty() {
//!         coverage_point!("empty data path");
//!         return;
//!     }
//!
//!     coverage_point!("processing complete");
//! }
//! ```

mod common;

#[cfg(feature = "enabled")]
pub mod enabled;

#[cfg(not(feature = "enabled"))]
mod disabled;

// Re-export common types
pub use common::{CoveragePoint, CoverageSnapshot, CoverageSummary, FileCoverage, ModuleCoverage};

// Re-export from the appropriate implementation module
#[cfg(feature = "enabled")]
pub use enabled::{
    coverage_by_file, coverage_by_module, coverage_percentage, coverage_summary, covered_points,
    covered_points_report, format_coverage_report, format_uncovered_report, get_coverage_report,
    print_coverage_report, print_uncovered_report, reset_coverage, total_points, uncovered_points,
    COVERAGE_CATALOG,
};

#[cfg(not(feature = "enabled"))]
pub use disabled::{
    coverage_by_file, coverage_by_module, coverage_percentage, coverage_summary, covered_points,
    covered_points_report, format_coverage_report, format_uncovered_report, get_coverage_report,
    print_coverage_report, print_uncovered_report, reset_coverage, total_points, uncovered_points,
};
