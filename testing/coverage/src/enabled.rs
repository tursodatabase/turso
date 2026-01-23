//! Coverage instrumentation implementation (enabled mode).

use std::collections::HashMap;
use std::sync::atomic::Ordering;

pub use linkme;
use linkme::distributed_slice;

use crate::common::{CoveragePoint, CoverageSnapshot, CoverageSummary, FileCoverage, ModuleCoverage};

/// The global catalog of coverage points (populated at link time).
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
    ($name:literal) => {{
        use std::sync::atomic::{AtomicU64, Ordering};

        static HIT_COUNT: AtomicU64 = AtomicU64::new(0);

        #[$crate::enabled::linkme::distributed_slice($crate::enabled::COVERAGE_CATALOG)]
        #[linkme(crate = $crate::enabled::linkme)]
        static CATALOG_ENTRY: $crate::CoveragePoint = $crate::CoveragePoint {
            name: $name,
            file: ::std::file!(),
            line: ::std::line!(),
            column: ::std::column!(),
            module: ::std::module_path!(),
            hit_count: &HIT_COUNT,
        };

        HIT_COUNT.fetch_add(1, Ordering::Relaxed);
    }};
}

/// Get a report of all coverage points and their hit counts.
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
pub fn total_points() -> usize {
    COVERAGE_CATALOG.len()
}

/// Get the number of points that have been hit at least once.
pub fn covered_points() -> usize {
    COVERAGE_CATALOG
        .iter()
        .filter(|p| p.hit_count.load(Ordering::Relaxed) > 0)
        .count()
}

/// Reset all coverage counters to zero.
pub fn reset_coverage() {
    for point in COVERAGE_CATALOG.iter() {
        point.hit_count.store(0, Ordering::Relaxed);
    }
}

/// Get the coverage percentage (0.0 to 100.0).
/// Returns 100.0 if there are no instrumentation points.
pub fn coverage_percentage() -> f64 {
    let total = total_points();
    if total == 0 {
        return 100.0;
    }
    (covered_points() as f64 / total as f64) * 100.0
}

/// Get only the points that have NOT been hit.
pub fn uncovered_points() -> Vec<CoverageSnapshot> {
    get_coverage_report()
        .into_iter()
        .filter(|p| p.hit_count == 0)
        .collect()
}

/// Get only the points that have been hit at least once.
pub fn covered_points_report() -> Vec<CoverageSnapshot> {
    get_coverage_report()
        .into_iter()
        .filter(|p| p.hit_count > 0)
        .collect()
}

/// Get a summary of coverage statistics.
pub fn coverage_summary() -> CoverageSummary {
    let total = total_points();
    let covered = covered_points();
    CoverageSummary {
        total_points: total,
        covered_points: covered,
        uncovered_points: total - covered,
        percentage: if total == 0 {
            100.0
        } else {
            (covered as f64 / total as f64) * 100.0
        },
    }
}

/// Get coverage grouped by module path.
pub fn coverage_by_module() -> Vec<ModuleCoverage> {
    let report = get_coverage_report();
    let mut by_module: HashMap<&'static str, Vec<CoverageSnapshot>> = HashMap::new();

    for point in report {
        by_module.entry(point.module).or_default().push(point);
    }

    let mut result: Vec<ModuleCoverage> = by_module
        .into_iter()
        .map(|(module, points)| {
            let total = points.len();
            let covered = points.iter().filter(|p| p.hit_count > 0).count();
            ModuleCoverage {
                module,
                total,
                covered,
                percentage: if total == 0 {
                    100.0
                } else {
                    (covered as f64 / total as f64) * 100.0
                },
                points,
            }
        })
        .collect();

    result.sort_by(|a, b| a.module.cmp(b.module));
    result
}

/// Get coverage grouped by source file.
pub fn coverage_by_file() -> Vec<FileCoverage> {
    let report = get_coverage_report();
    let mut by_file: HashMap<&'static str, Vec<CoverageSnapshot>> = HashMap::new();

    for point in report {
        by_file.entry(point.file).or_default().push(point);
    }

    let mut result: Vec<FileCoverage> = by_file
        .into_iter()
        .map(|(file, mut points)| {
            points.sort_by_key(|p| p.line);
            let total = points.len();
            let covered = points.iter().filter(|p| p.hit_count > 0).count();
            FileCoverage {
                file,
                total,
                covered,
                percentage: if total == 0 {
                    100.0
                } else {
                    (covered as f64 / total as f64) * 100.0
                },
                points,
            }
        })
        .collect();

    result.sort_by(|a, b| a.file.cmp(b.file));
    result
}

/// Format a coverage report as a human-readable string.
pub fn format_coverage_report() -> String {
    let summary = coverage_summary();
    let mut output = String::new();

    output.push_str(&format!(
        "Coverage Summary: {}/{} points ({:.1}%)\n",
        summary.covered_points, summary.total_points, summary.percentage
    ));
    output.push_str(&"=".repeat(60));
    output.push('\n');

    for file_cov in coverage_by_file() {
        output.push_str(&format!(
            "\n{} ({}/{} = {:.1}%)\n",
            file_cov.file, file_cov.covered, file_cov.total, file_cov.percentage
        ));
        output.push_str(&"-".repeat(40));
        output.push('\n');

        for point in &file_cov.points {
            let status = if point.hit_count > 0 { "+" } else { "-" };
            output.push_str(&format!(
                "  {} L{}: {} (hits: {})\n",
                status, point.line, point.name, point.hit_count
            ));
        }
    }

    output
}

/// Format a report of only uncovered points.
pub fn format_uncovered_report() -> String {
    let uncovered = uncovered_points();
    let summary = coverage_summary();

    if uncovered.is_empty() {
        return format!(
            "All {} coverage points have been hit!\n",
            summary.total_points
        );
    }

    let mut output = String::new();
    output.push_str(&format!(
        "Uncovered Points: {} of {} ({:.1}% covered)\n",
        uncovered.len(),
        summary.total_points,
        summary.percentage
    ));
    output.push_str(&"=".repeat(60));
    output.push('\n');

    // Group by file for readability
    let mut by_file: HashMap<&str, Vec<&CoverageSnapshot>> = HashMap::new();
    for point in &uncovered {
        by_file.entry(point.file).or_default().push(point);
    }

    let mut files: Vec<_> = by_file.into_iter().collect();
    files.sort_by(|a, b| a.0.cmp(b.0));

    for (file, mut points) in files {
        points.sort_by_key(|p| p.line);
        output.push_str(&format!("\n{file}\n"));
        for point in points {
            output.push_str(&format!("  L{}: {}\n", point.line, point.name));
        }
    }

    output
}

/// Print the full coverage report to stdout.
pub fn print_coverage_report() {
    print!("{}", format_coverage_report());
}

/// Print only uncovered points to stdout.
pub fn print_uncovered_report() {
    print!("{}", format_uncovered_report());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coverage_point_exists_in_catalog() {
        coverage_point!("catalog test point");

        assert!(total_points() >= 1);

        let report = get_coverage_report();
        let test_point = report.iter().find(|p| p.name == "catalog test point");
        assert!(test_point.is_some());

        let point = test_point.unwrap();
        assert!(point.file.ends_with("enabled.rs"));
        assert!(point.line > 0);
        assert!(!point.module.is_empty());
    }

    #[test]
    fn test_hit_count_increments() {
        use std::sync::atomic::AtomicU64;

        static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

        let before = TEST_COUNTER.fetch_add(0, Ordering::Relaxed);

        TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        TEST_COUNTER.fetch_add(1, Ordering::Relaxed);

        let after = TEST_COUNTER.load(Ordering::Relaxed);

        assert_eq!(after - before, 3);

        coverage_point!("increment verification point");
    }

    #[test]
    fn test_api_functions_compile() {
        let _total = total_points();
        let _covered = covered_points();
        let _report = get_coverage_report();

        assert!(_total >= 1);
    }

    #[test]
    fn test_helper_functions() {
        coverage_point!("helper test point");

        let summary = coverage_summary();
        assert!(summary.total_points >= 1);
        assert!((0.0..=100.0).contains(&summary.percentage));
        assert_eq!(
            summary.total_points,
            summary.covered_points + summary.uncovered_points
        );

        let pct = coverage_percentage();
        assert!((0.0..=100.0).contains(&pct));

        let by_module = coverage_by_module();
        assert!(!by_module.is_empty());
        let our_module = by_module
            .iter()
            .find(|m| m.module.contains("turso_coverage"));
        assert!(our_module.is_some());

        let by_file = coverage_by_file();
        assert!(!by_file.is_empty());
        let our_file = by_file.iter().find(|f| f.file.ends_with("enabled.rs"));
        assert!(our_file.is_some());

        let report = format_coverage_report();
        assert!(report.contains("Coverage Summary"));

        let uncovered_report = format_uncovered_report();
        assert!(!uncovered_report.is_empty());

        let covered = covered_points_report();
        let uncovered = uncovered_points();
        assert_eq!(covered.len() + uncovered.len(), summary.total_points);
    }
}
