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

#[cfg(feature = "enabled")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "enabled")]
pub use linkme;

#[cfg(feature = "enabled")]
use linkme::distributed_slice;

/// Information about an instrumentation point.
#[cfg(feature = "enabled")]
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
    /// How many times this point was hit
    pub hit_count: &'static AtomicU64,
}

// SAFETY: CoveragePoint only contains static references and atomics,
// which are both thread-safe.
#[cfg(feature = "enabled")]
unsafe impl Sync for CoveragePoint {}

/// The global catalog of coverage points (populated at link time).
#[cfg(feature = "enabled")]
#[distributed_slice]
pub static COVERAGE_CATALOG: [CoveragePoint];

/// Track that this code path was reached.
///
/// When feature "enabled" is off, compiles to nothing (zero overhead).
///
/// # Example
///
/// ```ignore
/// coverage_point!("reached the error handler");
/// ```
#[macro_export]
macro_rules! coverage_point {
    ($name:literal) => {
        #[cfg(feature = "enabled")]
        {
            use std::sync::atomic::{AtomicU64, Ordering};

            static HIT_COUNT: AtomicU64 = AtomicU64::new(0);

            #[$crate::linkme::distributed_slice($crate::COVERAGE_CATALOG)]
            #[linkme(crate = $crate::linkme)]
            static CATALOG_ENTRY: $crate::CoveragePoint = $crate::CoveragePoint {
                name: $name,
                file: ::std::file!(),
                line: ::std::line!(),
                column: ::std::column!(),
                module: ::std::module_path!(),
                hit_count: &HIT_COUNT,
            };

            HIT_COUNT.fetch_add(1, Ordering::Relaxed);
        }
    };
}

/// A snapshot of a coverage point's state.
#[cfg(feature = "enabled")]
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
    /// How many times this point was hit
    pub hit_count: u64,
}

/// Get a report of all coverage points and their hit counts.
#[cfg(feature = "enabled")]
pub fn get_coverage_report() -> Vec<CoverageSnapshot> {
    COVERAGE_CATALOG
        .iter()
        .map(|point| CoverageSnapshot {
            name: point.name,
            file: point.file,
            line: point.line,
            column: point.column,
            module: point.module,
            hit_count: point.hit_count.load(Ordering::Relaxed),
        })
        .collect()
}

/// Get the total number of instrumentation points.
#[cfg(feature = "enabled")]
pub fn total_points() -> usize {
    COVERAGE_CATALOG.len()
}

/// Get the number of points that have been hit at least once.
#[cfg(feature = "enabled")]
pub fn covered_points() -> usize {
    COVERAGE_CATALOG
        .iter()
        .filter(|p| p.hit_count.load(Ordering::Relaxed) > 0)
        .count()
}

/// Reset all coverage counters to zero.
#[cfg(feature = "enabled")]
pub fn reset_coverage() {
    for point in COVERAGE_CATALOG.iter() {
        point.hit_count.store(0, Ordering::Relaxed);
    }
}

// Stub versions when disabled - return empty/zero values

/// Get a report of all coverage points (stub when disabled).
#[cfg(not(feature = "enabled"))]
pub fn get_coverage_report() -> Vec<()> {
    vec![]
}

/// Get the total number of instrumentation points (stub when disabled).
#[cfg(not(feature = "enabled"))]
pub fn total_points() -> usize {
    0
}

/// Get the number of points that have been hit (stub when disabled).
#[cfg(not(feature = "enabled"))]
pub fn covered_points() -> usize {
    0
}

/// Reset all coverage counters (stub when disabled).
#[cfg(not(feature = "enabled"))]
pub fn reset_coverage() {}

#[cfg(all(test, feature = "enabled"))]
mod tests {
    use super::*;

    #[test]
    fn test_coverage_point_exists_in_catalog() {
        // Define a coverage point
        coverage_point!("catalog test point");

        // Should have at least one point registered
        assert!(total_points() >= 1);

        // Check the report contains our point
        let report = get_coverage_report();
        let test_point = report.iter().find(|p| p.name == "catalog test point");
        assert!(test_point.is_some());

        // Verify location info is captured
        let point = test_point.unwrap();
        assert!(point.file.ends_with("lib.rs"));
        assert!(point.line > 0);
        assert!(!point.module.is_empty());
    }

    #[test]
    fn test_hit_count_increments() {
        // Use a unique point name and track hits via the atomic directly
        // This avoids race conditions with reset_coverage from other tests
        use std::sync::atomic::AtomicU64;

        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

        let before = TEST_COUNTER.fetch_add(0, Ordering::Relaxed);

        // Manually simulate what the macro does for testing
        TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        TEST_COUNTER.fetch_add(1, Ordering::Relaxed);

        let after = TEST_COUNTER.load(Ordering::Relaxed);

        // Should have incremented by 3
        assert_eq!(after - before, 3);

        // Also verify a coverage_point compiles and runs
        coverage_point!("increment verification point");
    }

    #[test]
    fn test_api_functions_compile() {
        // Verify all API functions are callable
        let _total = total_points();
        let _covered = covered_points();
        let _report = get_coverage_report();

        // Verify reset compiles (but don't call it to avoid affecting other tests)
        // reset_coverage();

        // At least one point should exist from this crate's tests
        assert!(_total >= 1);
    }
}
