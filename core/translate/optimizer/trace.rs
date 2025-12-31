//! WHERETRACE-style optimizer debugging for Turso.
//!
//! Provides SQLite-compatible bitmask-filtered trace output for query optimizer decisions.
//!
//! # Usage
//!
//! Build with the `wheretrace` feature enabled:
//! ```bash
//! cargo build --features wheretrace
//! ```
//!
//! Set the trace mask via environment variable:
//! ```bash
//! # All traces
//! TURSO_WHERETRACE=0xFFFF ./turso db.sqlite < query.sql
//!
//! # Only cost calculations
//! TURSO_WHERETRACE=0x04 ./turso db.sqlite < query.sql
//!
//! # Only Turso-specific traces (hash join, bloom filter, etc.)
//! TURSO_WHERETRACE=0x0F00 ./turso db.sqlite < query.sql
//!
//! # SQLite-compatible flags only
//! TURSO_WHERETRACE=0xC03F ./turso db.sqlite < query.sql
//! ```

use std::sync::atomic::{AtomicU32, Ordering};

/// Global trace mask, set via TURSO_WHERETRACE env var.
pub static WHERETRACE: AtomicU32 = AtomicU32::new(0);

/// Trace category flags (matching SQLite where applicable, plus Turso-specific).
///
/// The bitmask values are chosen to be compatible with SQLite's WHERETRACE
/// where possible, making it easier to compare outputs between the two systems.
#[allow(dead_code)]
pub mod flags {
    // =========================================================================
    // SQLite-compatible flags (0x0001 - 0x00FF, plus 0x4000, 0x8000)
    // =========================================================================

    /// High-level optimizer start/end messages.
    /// Shows: `*** Optimizer Start ***`, `*** Optimizer Finished ***`
    pub const SUMMARY: u32 = 0x0001;

    /// DP solver exploration of join orderings.
    /// Shows: `---- begin solver.`, `---- after round N ----`, cost updates
    pub const SOLVER: u32 = 0x0002;

    /// Cost calculations and estimates.
    /// Shows: selectivity, IO cost, CPU cost, total cost for each candidate
    pub const COST: u32 = 0x0004;

    /// Join candidate add/skip/replace decisions.
    /// Shows: `add: table=N cost=X`, `skip: table=N cost=X > bound=Y`
    pub const LOOP_INSERT: u32 = 0x0008;

    /// Index/access method selection.
    /// Shows: all candidates considered, which one wins, `BEGIN/END addBtreeIdx()`
    pub const ACCESS_METHOD: u32 = 0x0010;

    /// Constraint analysis and selectivity.
    /// Shows: each WHERE term extracted, operator, selectivity, usability
    pub const CONSTRAINT: u32 = 0x0020;

    /// Rejection reasons for alternative plans.
    /// Shows: `[SKIP] {table} cost={cost} > bound={bound} reason={reason}`
    pub const REJECTED: u32 = 0x0040;

    /// Query structure (FROM, WHERE tree, ORDERBY).
    /// Shows: table references, WHERE clause as expression tree
    pub const QUERY_STRUCTURE: u32 = 0x4000;

    /// Code generation phase.
    /// Shows: `Coding level N`, term consumption, `DISABLE-TERM`
    pub const CODE_GEN: u32 = 0x8000;

    // =========================================================================
    // Turso-specific flags (0x0100 - 0x0F00)
    // =========================================================================

    /// Hash join build/probe decisions.
    /// Shows: candidate evaluation, equi-join keys, cost comparison vs nested loop
    pub const HASH_JOIN: u32 = 0x0100;

    /// Bloom filter creation/usage.
    /// Shows: filter creation, probe decisions, false positive estimates
    pub const BLOOM_FILTER: u32 = 0x0200;

    /// Ephemeral index build decisions.
    /// Shows: when temporary indexes are built, build costs
    pub const EPHEMERAL_IDX: u32 = 0x0400;

    /// ORDER BY satisfaction analysis.
    /// Shows: target columns, whether index order matches, sort elimination
    pub const ORDER_TARGET: u32 = 0x0800;

    // =========================================================================
    // Convenience masks
    // =========================================================================

    /// All trace categories enabled
    pub const ALL: u32 = 0xFFFF;

    /// Only SQLite-compatible flags (for comparing with SQLite output)
    pub const SQLITE_COMPAT: u32 = SUMMARY
        | SOLVER
        | COST
        | LOOP_INSERT
        | ACCESS_METHOD
        | CONSTRAINT
        | QUERY_STRUCTURE
        | CODE_GEN;

    /// Just Turso-specific flags
    pub const TURSO_ONLY: u32 = HASH_JOIN | BLOOM_FILTER | EPHEMERAL_IDX | ORDER_TARGET;

    /// Core optimizer decisions (most useful for debugging query plans)
    pub const CORE: u32 = SUMMARY | SOLVER | LOOP_INSERT | ACCESS_METHOD | REJECTED;
}

/// Output format for traces (defaults to human-readable badges)
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OutputFormat {
    /// [ADD] [SKIP] [HASH] style badges with colors
    Badges,
    /// JSON Lines format for programmatic parsing
    Json,
}

static OUTPUT_FORMAT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Get the current output format.
#[inline]
pub fn output_format() -> OutputFormat {
    match OUTPUT_FORMAT.load(Ordering::Relaxed) {
        1 => OutputFormat::Json,
        _ => OutputFormat::Badges,
    }
}

/// Initialize the trace mask from the `TURSO_WHERETRACE` environment variable.
///
/// Call this once at startup (e.g., in main or connection setup).
/// The value can be decimal or hex (with `0x` prefix).
///
/// # Examples
///
/// ```bash
/// TURSO_WHERETRACE=0xFFFF  # All traces (hex)
/// TURSO_WHERETRACE=65535   # All traces (decimal)
/// TURSO_WHERETRACE=0x0001  # Just summary
/// ```
pub fn init_from_env() {
    if let Ok(val) = std::env::var("TURSO_WHERETRACE") {
        let trimmed = val.trim();
        let mask = if let Some(hex) = trimmed
            .strip_prefix("0x")
            .or_else(|| trimmed.strip_prefix("0X"))
        {
            u32::from_str_radix(hex, 16).unwrap_or(0)
        } else {
            trimmed.parse::<u32>().unwrap_or(0)
        };
        WHERETRACE.store(mask, Ordering::Relaxed);
    }

    // Check for output format override
    if let Ok(fmt) = std::env::var("TURSO_WHERETRACE_FORMAT") {
        if fmt.eq_ignore_ascii_case("json") {
            OUTPUT_FORMAT.store(1, Ordering::Relaxed);
        }
    }
}

/// Check if a trace category is enabled.
#[inline(always)]
pub fn enabled(mask: u32) -> bool {
    (WHERETRACE.load(Ordering::Relaxed) & mask) != 0
}

/// Main trace macro - outputs to stderr when enabled.
///
/// This macro is a no-op when the `wheretrace` feature is disabled,
/// ensuring zero overhead in release builds.
///
/// # Example
/// ```ignore
/// wheretrace!(flags::COST, "cost: table={} total={:.1}", table_name, cost);
/// ```
#[macro_export]
macro_rules! wheretrace {
    ($mask:expr, $($arg:tt)*) => {
        if $crate::translate::optimizer::trace::enabled($mask) {
            eprintln!($($arg)*);
        }
    };
}

/// Trace macro with explicit function name prefix.
/// Shows: `'-- function_name()` followed by the trace content.
///
/// This requires manually passing the function name since Rust doesn't have
/// a built-in `function!()` macro like C's `__func__`.
///
/// # Example
/// ```ignore
/// wheretrace_fn!("constraints_from_where_clause", flags::QUERY_STRUCTURE, "FROM clause tables:");
/// // Output: '-- constraints_from_where_clause()
/// //         FROM clause tables:
/// ```
#[macro_export]
macro_rules! wheretrace_fn {
    ($fn_name:literal, $mask:expr, $($arg:tt)*) => {
        if $crate::translate::optimizer::trace::enabled($mask) {
            eprintln!("'-- {}()", $fn_name);
            eprintln!($($arg)*);
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enabled_when_mask_matches() {
        WHERETRACE.store(flags::COST | flags::SOLVER, Ordering::Relaxed);
        assert!(enabled(flags::COST));
        assert!(enabled(flags::SOLVER));
        assert!(!enabled(flags::HASH_JOIN));
        WHERETRACE.store(0, Ordering::Relaxed);
    }

    #[test]
    fn test_enabled_when_all() {
        WHERETRACE.store(flags::ALL, Ordering::Relaxed);
        assert!(enabled(flags::COST));
        assert!(enabled(flags::HASH_JOIN));
        assert!(enabled(flags::BLOOM_FILTER));
        assert!(enabled(flags::QUERY_STRUCTURE));
        assert!(enabled(flags::CODE_GEN));
        WHERETRACE.store(0, Ordering::Relaxed);
    }

    #[test]
    fn test_convenience_masks() {
        // SQLITE_COMPAT should include all SQLite-compatible flags
        assert!(flags::SQLITE_COMPAT & flags::SUMMARY != 0);
        assert!(flags::SQLITE_COMPAT & flags::QUERY_STRUCTURE != 0);
        assert!(flags::SQLITE_COMPAT & flags::CODE_GEN != 0);
        // But not Turso-specific flags
        assert!(flags::SQLITE_COMPAT & flags::HASH_JOIN == 0);

        // TURSO_ONLY should include only Turso-specific flags
        assert!(flags::TURSO_ONLY & flags::HASH_JOIN != 0);
        assert!(flags::TURSO_ONLY & flags::BLOOM_FILTER != 0);
        // But not SQLite-compatible flags
        assert!(flags::TURSO_ONLY & flags::SUMMARY == 0);
    }
}
