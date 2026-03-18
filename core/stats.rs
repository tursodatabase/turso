use crate::sync::Arc;
use crate::translate::collate::CollationSeq;
use rustc_hash::FxHashMap as HashMap;

use crate::schema::Schema;
use crate::translate::emitter::TransactionMode;
use crate::util::normalize_ident;
use crate::{Connection, Result, Statement, TransactionState, Value};
pub const STATS_TABLE: &str = "sqlite_stat1";
pub const STATS4_TABLE: &str = "sqlite_stat4";
const STATS_QUERY: &str = "SELECT tbl, idx, stat FROM sqlite_stat1";
const STATS4_QUERY: &str = "SELECT tbl, idx, neq, nlt, ndlt, sample FROM sqlite_stat4";

/// A single sample row from sqlite_stat4, representing a histogram bucket.
#[derive(Clone, Debug)]
pub struct Stat4IndexSample {
    /// Raw record blob (SQLite record format). Used for binary search comparisons.
    pub sample: Vec<u8>,
    /// nEq per column prefix: how many rows have the exact same value(s) as this sample.
    pub n_eq: Vec<u64>,
    /// nLt per column prefix: how many rows have values less than this sample.
    pub n_lt: Vec<u64>,
    /// nDLt per column prefix: how many distinct values are less than this sample.
    pub n_distinct_lt: Vec<u64>,
}

/// Statistics produced by ANALYZE for a single index.
#[derive(Clone, Debug, Default)]
pub struct IndexStat {
    /// Estimated total number of rows in the table/index when the stat was collected.
    pub total_rows: Option<u64>,
    /// Average number of rows per distinct key prefix, for each leftmost prefix
    /// of the index columns.
    ///
    /// These values come directly from sqlite_stat1's stat column (after the
    /// first number which is total_rows). For a stat string "1000 100 10 1":
    /// - total_rows = 1000
    /// - avg_rows_per_distinct_prefix = [100, 10, 1]
    ///
    /// Entry at position `i` means: on average, this many rows share the same
    /// values in the first `i + 1` columns of the index. Lower values indicate
    /// higher selectivity (more distinct prefixes).
    ///
    /// To compute number of distinct values (NDV) for prefix i:
    ///   ndv = total_rows / avg_rows_per_distinct_prefix[i]
    pub avg_rows_per_distinct_prefix: Vec<u64>,
    /// sqlite_stat4 histogram samples, sorted by sample key (ascending nLt[last]).
    pub samples: Vec<Stat4IndexSample>,
    /// Average nEq for values that do NOT appear in the samples, per column prefix.
    /// Used as the fallback estimate for between-sample lookups.
    pub avg_eq: Vec<f64>,
}

/// Statistics produced by ANALYZE for a single BTree table.
#[derive(Clone, Debug, Default)]
pub struct TableStat {
    /// Estimated row count for the table (sqlite_stat1 entry with a NULL index name).
    pub row_count: Option<u64>,
    /// Per-index statistics keyed by normalized index name.
    pub index_stats: HashMap<String, IndexStat>,
}

impl TableStat {
    /// Get or create the per-index statistics bucket for the given index name.
    pub fn index_stats_mut(&mut self, index_name: &str) -> &mut IndexStat {
        let index_name = normalize_ident(index_name);
        self.index_stats.entry(index_name).or_default()
    }
}

/// Container for ANALYZE statistics across the schema.
#[derive(Clone, Debug, Default)]
pub struct AnalyzeStats {
    /// Per-table statistics keyed by normalized table name.
    pub tables: HashMap<String, TableStat>,
}

impl AnalyzeStats {
    pub fn needs_refresh(&self) -> bool {
        self.tables.is_empty()
    }
    /// Get the statistics for a table, if present.
    pub fn table_stats(&self, table_name: &str) -> Option<&TableStat> {
        let table_name = normalize_ident(table_name);
        self.tables.get(&table_name)
    }

    /// Get or create the statistics bucket for a table.
    pub fn table_stats_mut(&mut self, table_name: &str) -> &mut TableStat {
        let table_name = normalize_ident(table_name);
        self.tables.entry(table_name).or_default()
    }

    /// Remove all statistics for a table.
    pub fn remove_table(&mut self, table_name: &str) {
        let table_name = normalize_ident(table_name);
        self.tables.remove(&table_name);
    }

    /// Remove statistics for a specific index on a table.
    pub fn remove_index(&mut self, table_name: &str, index_name: &str) {
        let table_name = normalize_ident(table_name);
        let index_name = normalize_ident(index_name);
        if let Some(table_stats) = self.tables.get_mut(&table_name) {
            table_stats.index_stats.remove(&index_name);
        }
    }
}

/// Read sqlite_stat1 (and optionally stat4) contents into an AnalyzeStats map
/// without mutating schema.
///
/// Only regular B-tree tables and indexes are considered. Virtual and ephemeral
/// tables are ignored.
pub fn gather_statistics(
    conn: &Arc<Connection>,
    schema: &Schema,
    mv_tx: Option<(u64, TransactionMode)>,
) -> Result<AnalyzeStats> {
    let mut stats = AnalyzeStats::default();
    let mut stmt = conn.prepare(STATS_QUERY)?;
    stmt.set_mv_tx(mv_tx);
    load_sqlite_stat1_from_stmt(stmt, schema, &mut stats)?;

    // Load stat4 if the table exists.
    if schema.get_btree_table(STATS4_TABLE).is_some() {
        let stmt4 = conn.prepare(STATS4_QUERY)?;
        load_sqlite_stat4_from_stmt(stmt4, schema, &mut stats)?;
    }

    Ok(stats)
}

/// Refresh analyze_stats on the connection's schema.
/// Returns Ok(()) if stats were refreshed (or no stat table exists).
pub fn refresh_analyze_stats(conn: &Arc<Connection>) -> Result<()> {
    if !conn.is_db_initialized() || conn.is_nested_stmt() {
        return Ok(());
    }
    if matches!(conn.get_tx_state(), TransactionState::Write { .. }) {
        return Ok(());
    }

    // Need a snapshot of the current schema to validate tables/indexes.
    let schema_snapshot = { conn.schema.read().clone() };
    if schema_snapshot.get_btree_table(STATS_TABLE).is_none() {
        return Ok(());
    }

    let mv_tx = conn.get_mv_tx();
    let stats = gather_statistics(conn, &schema_snapshot, mv_tx)?;
    conn.with_schema_mut(|schema| {
        schema.analyze_stats = stats;
    });
    Ok(())
}

fn load_sqlite_stat1_from_stmt(
    mut stmt: Statement,
    schema: &Schema,
    stats: &mut AnalyzeStats,
) -> Result<()> {
    stmt.run_with_row_callback(|row| {
        let table_name = row.get::<&str>(0)?;
        let idx_value = row.get::<&Value>(1)?;
        let stat_value = row.get::<&Value>(2)?;

        let idx_name = match idx_value {
            Value::Null => None,
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        };
        let stat = match stat_value {
            Value::Text(s) => s.as_str(),
            _ => return Err(crate::LimboError::InternalError(
                format!("sqlite_stat1: expected text for stat column, got {stat_value}")
            )),
        };

        // Skip if table is not a regular B-tree (may have been dropped after ANALYZE).
        if schema.get_btree_table(table_name).is_none() {
            return Ok(());
        }
        let Some(numbers) = parse_stat_numbers(stat) else {
            return Err(crate::LimboError::InternalError(
                format!("sqlite_stat1: malformed stat string: {stat:?}")
            ));
        };
        if numbers.is_empty() {
            return Err(crate::LimboError::InternalError(
                format!("sqlite_stat1: empty stat string for table {table_name}")
            ));
        }
        if idx_name.is_none() {
            if let Some(total_rows) = numbers.first().copied() {
                stats.table_stats_mut(table_name).row_count = Some(total_rows);
            }
            return Ok(());
        }

        // Index-level entry: only keep if the index exists on this table.
        let idx_name = normalize_ident(idx_name.unwrap());
        if schema.get_index(table_name, &idx_name).is_none() {
            return Ok(());
        }

        let total_rows = numbers.first().copied();
        {
            let idx_stats = stats.table_stats_mut(table_name).index_stats_mut(&idx_name);
            idx_stats.total_rows = total_rows;
            idx_stats.avg_rows_per_distinct_prefix = numbers.iter().skip(1).copied().collect();
        }

        // If we didn't see a table-level row yet, seed row_count from index stats.
        if let Some(total_rows) = total_rows {
            let table_stats = stats.table_stats_mut(table_name);
            if table_stats.row_count.is_none() {
                table_stats.row_count = Some(total_rows);
            }
        }
        Ok(())
    })?;

    Ok(())
}

fn parse_stat_numbers(stat: &str) -> Option<Vec<u64>> {
    stat.split_whitespace()
        .map(|s| s.parse::<u64>().ok())
        .collect()
}

/// Load sqlite_stat4 histogram samples into the AnalyzeStats.
/// Must be called after stat1 has been loaded so IndexStat entries exist.
fn load_sqlite_stat4_from_stmt(
    mut stmt: Statement,
    schema: &Schema,
    stats: &mut AnalyzeStats,
) -> Result<()> {
    stmt.run_with_row_callback(|row| {
        let table_name = row.get::<&str>(0)?;
        let idx_value = row.get::<&Value>(1)?;
        let neq_value = row.get::<&Value>(2)?;
        let nlt_value = row.get::<&Value>(3)?;
        let ndlt_value = row.get::<&Value>(4)?;
        let sample_value = row.get::<&Value>(5)?;

        let idx_name = match idx_value {
            Value::Text(s) => s.as_str(),
            _ => return Err(crate::LimboError::InternalError(
                format!("sqlite_stat4: expected text for idx column, got {idx_value}")
            )),
        };

        // Skip if table or index doesn't exist in the schema (may have been dropped after ANALYZE).
        if schema.get_btree_table(table_name).is_none() {
            return Ok(());
        }
        let idx_name = normalize_ident(idx_name);
        if schema.get_index(table_name, &idx_name).is_none() {
            return Ok(());
        }

        let neq_str = match neq_value {
            Value::Text(s) => s.as_str(),
            _ => return Err(crate::LimboError::InternalError(
                format!("sqlite_stat4: expected text for neq column, got {neq_value}")
            )),
        };
        let nlt_str = match nlt_value {
            Value::Text(s) => s.as_str(),
            _ => return Err(crate::LimboError::InternalError(
                format!("sqlite_stat4: expected text for nlt column, got {nlt_value}")
            )),
        };
        let ndlt_str = match ndlt_value {
            Value::Text(s) => s.as_str(),
            _ => return Err(crate::LimboError::InternalError(
                format!("sqlite_stat4: expected text for ndlt column, got {ndlt_value}")
            )),
        };
        let sample_blob = match sample_value {
            Value::Blob(b) => b.clone(),
            _ => return Err(crate::LimboError::InternalError(
                format!("sqlite_stat4: expected blob for sample column, got {sample_value}")
            )),
        };

        let Some(n_eq) = parse_stat_numbers(neq_str) else {
            return Err(crate::LimboError::InternalError(
                format!("sqlite_stat4: malformed neq string: {neq_str:?}")
            ));
        };
        let Some(n_lt) = parse_stat_numbers(nlt_str) else {
            return Err(crate::LimboError::InternalError(
                format!("sqlite_stat4: malformed nlt string: {nlt_str:?}")
            ));
        };
        let Some(n_distinct_lt) = parse_stat_numbers(ndlt_str) else {
            return Err(crate::LimboError::InternalError(
                format!("sqlite_stat4: malformed ndlt string: {ndlt_str:?}")
            ));
        };

        let idx_stats = stats.table_stats_mut(table_name).index_stats_mut(&idx_name);
        idx_stats.samples.push(Stat4IndexSample {
            sample: sample_blob,
            n_eq,
            n_lt,
            n_distinct_lt,
        });

        Ok(())
    })?;

    // For each index that has samples, compute avg_eq and sort samples.
    for table_stats in stats.tables.values_mut() {
        for idx_stats in table_stats.index_stats.values_mut() {
            if idx_stats.samples.is_empty() {
                continue;
            }
            // Sort samples by nLt[last] (full-key order), matching SQLite's behavior.
            // nLt[last] is the rowid column's nLt which gives total ordering.
            idx_stats
                .samples
                .sort_by(|a, b| a.n_lt.last().cmp(&b.n_lt.last()));

            compute_avg_eq(idx_stats);
        }
    }

    Ok(())
}

/// Compute avg_eq: the average nEq for values that are NOT in the samples.
/// This is the fallback estimate for between-sample lookups, matching SQLite's aAvgEq.
fn compute_avg_eq(idx_stats: &mut IndexStat) {
    let total_rows = idx_stats.total_rows.unwrap_or(0) as f64;
    if idx_stats.samples.is_empty() || total_rows <= 0.0 {
        return;
    }

    let n_cols = idx_stats
        .samples
        .iter()
        .map(|s| s.n_eq.len())
        .max()
        .unwrap_or(0);

    idx_stats.avg_eq = Vec::with_capacity(n_cols);
    for col_idx in 0..n_cols {
        // Sum of nEq for sampled values at this column prefix
        let sampled_eq_sum: f64 = idx_stats
            .samples
            .iter()
            .filter_map(|s| s.n_eq.get(col_idx))
            .map(|&v| v as f64)
            .sum();
        // Number of distinct sampled values at this column prefix
        let n_samples = idx_stats.samples.len() as f64;
        // Total distinct values at this prefix from stat1
        let n_distinct = if col_idx < idx_stats.avg_rows_per_distinct_prefix.len() {
            let avg = idx_stats.avg_rows_per_distinct_prefix[col_idx] as f64;
            if avg > 0.0 {
                total_rows / avg
            } else {
                total_rows
            }
        } else {
            total_rows
        };
        // Non-sampled distinct values
        let non_sampled_distinct = (n_distinct - n_samples).max(1.0);
        // Non-sampled rows
        let non_sampled_rows = (total_rows - sampled_eq_sum).max(0.0);
        // Average rows per non-sampled distinct value
        let avg = non_sampled_rows / non_sampled_distinct;
        idx_stats.avg_eq.push(avg.max(1.0));
    }
}

/// Estimate equality row count for a multi-column prefix using stat4 histogram samples.
///
/// Performs a binary search over the sorted samples, comparing all K probe values
/// field-by-field against each sample's first K columns. If an exact match on all
/// K columns is found, returns that sample's `nEq[K-1]`. Otherwise returns
/// `avg_eq[K-1]` (the average nEq for non-sampled values at that prefix depth).
///
/// `probe_values` contains the constant values for the leading equality columns.
/// `collations` contains the per-column collation sequence.
///
/// Returns `None` if samples are empty or sample blobs cannot be decoded.
pub fn stat4_equality_estimate(
    idx_stats: &IndexStat,
    probe_values: &[Value],
    collations: &[CollationSeq],
) -> Option<f64> {
    if idx_stats.samples.is_empty() || probe_values.is_empty() {
        return None;
    }

    let k = probe_values.len();

    // Binary search: compare first K columns of each sample against probe_values.
    // Uses ValueIterator on borrowed sample bytes to avoid cloning each sample blob.
    let had_decode_error = std::cell::Cell::new(false);
    let result = idx_stats.samples.binary_search_by(|sample| {
        let Ok(iter) = crate::types::ValueIterator::new(&sample.sample) else {
            had_decode_error.set(true);
            return std::cmp::Ordering::Equal;
        };
        for (i, val_result) in iter.enumerate().take(k) {
            let Ok(sample_val) = val_result else {
                had_decode_error.set(true);
                return std::cmp::Ordering::Equal;
            };
            let collation = collations.get(i).copied().unwrap_or_default();
            let cmp =
                crate::types::compare_immutable_single(sample_val, &probe_values[i], collation);
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });

    // If any sample failed to decode, bail out entirely and fall through to stat1.
    if had_decode_error.get() {
        return None;
    }

    let col_idx = k - 1;
    match result {
        Ok(idx) => {
            // Exact match on all K columns — return this sample's nEq[K-1]
            let sample = &idx_stats.samples[idx];
            let n_eq = sample.n_eq.get(col_idx).copied().unwrap_or(1);
            Some(n_eq as f64)
        }
        Err(_) => {
            // Between samples — return avg_eq for non-sampled values at prefix depth K
            let avg = idx_stats.avg_eq.get(col_idx).copied().unwrap_or(1.0);
            Some(avg)
        }
    }
}

/// Estimate (nLt, nEq) for a single probe value on the first column of an index,
/// mirroring SQLite's `whereKeyStats` (where.c:1703).
///
/// Binary searches stat4 samples comparing the probe value against each sample's
/// first column. Returns estimated (rows_less_than, rows_equal_to).
///
/// `round_up` controls gap interpolation direction:
/// - `false`: gap/3 (conservative low — used for lower bound estimation)
/// - `true`:  gap*2/3 (conservative high — used for upper bound estimation)
///
/// Returns `None` if samples are empty or decoding fails.
pub fn stat4_key_stats(
    idx_stats: &IndexStat,
    probe: &Value,
    collation: CollationSeq,
    round_up: bool,
) -> Option<(u64, u64)> {
    if idx_stats.samples.is_empty() {
        return None;
    }
    let total_rows = idx_stats.total_rows.unwrap_or(0);
    if total_rows == 0 {
        return None;
    }

    let samples = &idx_stats.samples;
    let n_samples = samples.len();

    // Manual binary search: find the first sample >= probe, tracking iLower
    // (nLt + nEq of the largest sample < probe).
    let mut lo = 0;
    let mut hi = n_samples;
    let mut i_lower: u64 = 0;
    let mut exact_match: Option<usize> = None;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let sample = &samples[mid];

        let Ok(iter) = crate::types::ValueIterator::new(&sample.sample) else {
            return None;
        };
        let Some(Ok(sample_val)) = iter.into_iter().next() else {
            return None;
        };

        let cmp = crate::types::compare_immutable_single(sample_val, probe, collation);
        match cmp {
            std::cmp::Ordering::Less => {
                // sample < probe: track this as the latest "below" sample
                let n_lt = sample.n_lt.first().copied().unwrap_or(0);
                let n_eq = sample.n_eq.first().copied().unwrap_or(0);
                i_lower = n_lt + n_eq;
                lo = mid + 1;
            }
            std::cmp::Ordering::Equal => {
                exact_match = Some(mid);
                break;
            }
            std::cmp::Ordering::Greater => {
                hi = mid;
            }
        }
    }

    if let Some(idx) = exact_match {
        // Exact match — return this sample's nLt and nEq for column 0
        let sample = &samples[idx];
        let n_lt = sample.n_lt.first().copied().unwrap_or(0);
        let n_eq = sample.n_eq.first().copied().unwrap_or(1);
        Some((n_lt, n_eq))
    } else {
        // Between samples. lo == hi == insertion point (first sample > probe).
        let i_upper = if lo < n_samples {
            samples[lo].n_lt.first().copied().unwrap_or(total_rows)
        } else {
            total_rows
        };

        let i_gap = i_upper.saturating_sub(i_lower);
        let interpolated = if round_up {
            i_lower + (i_gap * 2) / 3
        } else {
            i_lower + i_gap / 3
        };

        let avg_eq = idx_stats.avg_eq.first().copied().unwrap_or(1.0) as u64;
        Some((interpolated, avg_eq))
    }
}

/// Estimate the selectivity of a range scan using stat4 histogram data.
///
/// Given optional lower and upper bounds on the first column of an index,
/// computes `(iUpper - iLower) / total_rows` using `stat4_key_stats`.
/// Mirrors SQLite's `whereRangeScanEst` (where.c:2077).
///
/// Returns `None` if stat4 data is unavailable or bounds cannot be resolved.
pub fn stat4_range_selectivity(
    idx_stats: &IndexStat,
    lower: Option<(&Value, bool)>, // (value, is_strict: true for >, false for >=)
    upper: Option<(&Value, bool)>, // (value, is_strict: true for <, false for <=)
    collation: CollationSeq,
) -> Option<f64> {
    if lower.is_none() && upper.is_none() {
        return None;
    }
    let total_rows = idx_stats.total_rows.unwrap_or(0);
    if total_rows == 0 || idx_stats.samples.is_empty() {
        return None;
    }

    // Compute iLower: number of rows below the lower bound
    let i_lower = if let Some((val, is_strict)) = lower {
        let (n_lt, n_eq) = stat4_key_stats(idx_stats, val, collation, false)?;
        if is_strict {
            // > : exclude rows equal to bound
            n_lt + n_eq
        } else {
            // >= : include rows equal to bound
            n_lt
        }
    } else {
        0
    };

    // Compute iUpper: number of rows at or below the upper bound
    let i_upper = if let Some((val, is_strict)) = upper {
        let (n_lt, n_eq) = stat4_key_stats(idx_stats, val, collation, true)?;
        if is_strict {
            // < : exclude rows equal to bound
            n_lt
        } else {
            // <= : include rows equal to bound
            n_lt + n_eq
        }
    } else {
        total_rows
    };

    let range_rows = i_upper.saturating_sub(i_lower).max(1);
    Some(range_rows as f64 / total_rows as f64)
}

pub const STAT4_SAMPLES: usize = 24;

/// A collected stat4 sample with its per-column statistics.
/// Mirrors SQLite's StatSample struct.
#[derive(Debug, Clone)]
pub struct Stat4AccumSample {
    /// The sample key (index column values + rowid) as a record blob.
    pub key: Vec<u8>,
    /// nEq per column prefix. All vectors have `n_col` entries (nKeyCol + rowid).
    pub n_eq: Vec<u64>,
    /// nLt per column prefix.
    pub n_lt: Vec<u64>,
    /// nDLt per column prefix.
    pub n_distinct_lt: Vec<u64>,
    /// Whether this is a periodic sample (cannot be evicted by best-sample).
    pub is_periodic: bool,
    /// Which column this sample is the "best" for (used in sampleIsBetter).
    /// For periodic samples, iCol = 0.
    pub i_col: usize,
    /// PRNG hash at the time this sample was captured (SQLite's iHash).
    /// Used as the final tiebreaker in sampleIsBetterPost.
    pub i_hash: u32,
}

/// Statistics accumulator for ANALYZE.
/// Mirrors SQLite's StatAccum struct. `n_col` here matches SQLite's `nCol`
/// (= nKeyCol + 1, i.e., includes the rowid column). `n_key_col` is the
/// number of user-defined key columns (without rowid).
#[derive(Debug, Clone)]
pub struct StatAccum {
    /// Number of ALL columns (key columns + rowid). Matches SQLite's nCol.
    pub n_col: usize,
    /// Number of key columns (without rowid). Matches SQLite's nKeyCol.
    pub n_key_col: usize,
    /// Total number of rows seen.
    pub n_row: u64,
    /// Distinct counts for each column prefix (only n_key_col entries, for stat1).
    pub distinct: Vec<u64>,

    // --- stat4 sampling fields ---
    /// Whether stat4 collection is enabled.
    pub collect_stat4: bool,
    /// Maximum number of samples to collect (mxSample).
    pub max_samples: usize,
    /// Current row's running nEq counters per column prefix (n_col entries).
    pub current_n_eq: Vec<u64>,
    /// Current row's running nLt counters per column prefix (n_col entries).
    pub current_n_lt: Vec<u64>,
    /// Current row's running nDLt counters per column prefix (n_col entries).
    pub current_n_distinct_lt: Vec<u64>,
    /// Collected samples array (SQLite's p->a[]).
    pub samples: Vec<Stat4AccumSample>,
    /// Per-key-column best candidate (SQLite's aBest[], nCol-1 entries = n_key_col).
    pub best: Vec<Stat4AccumSample>,
    /// Index of the current minimum (least desirable) non-periodic sample.
    pub i_min: usize,
    /// Maximum number of leading zero anEq[] entries across all samples.
    pub n_max_eq_zero: usize,
    /// Periodic sampling interval (nPSample).
    pub n_psample: u64,
    /// The key blob of the current row.
    pub current_key: Vec<u8>,
    /// PRNG state for hash-based tiebreaking (iPrn).
    pub i_prn: u32,
    /// Hash of the current row's sample.
    pub current_hash: u32,
    /// Whether finalize_stat4() has already been called.
    stat4_finalized: bool,
}

impl StatAccum {
    /// Create a stat1-only accumulator. `n_key_col` is the number of index
    /// key columns (without rowid).
    pub fn new(n_key_col: usize) -> Self {
        let n_col = n_key_col + 1; // SQLite convention: nCol = nKeyCol + 1
        Self {
            n_col,
            n_key_col,
            n_row: 0,
            distinct: vec![0; n_key_col],
            collect_stat4: false,
            max_samples: 0,
            current_n_eq: Vec::new(),
            current_n_lt: Vec::new(),
            current_n_distinct_lt: vec![0; n_col],
            samples: Vec::new(),
            best: Vec::new(),
            i_min: 0,
            n_max_eq_zero: 0,
            n_psample: 0,
            current_key: Vec::new(),
            i_prn: 0,
            current_hash: 0,
            stat4_finalized: false,
        }
    }

    /// Create a stat4-enabled accumulator. `n_key_col` is the number of index
    /// key columns (without rowid).
    pub fn new_with_stat4(n_key_col: usize, estimated_rows: u64) -> Self {
        let n_col = n_key_col + 1;
        let max_samples = STAT4_SAMPLES;
        let n_psample = estimated_rows / (max_samples as u64 / 3 + 1) + 1;
        // Initialize PRNG like SQLite: iPrn = 0x689e962d * nCol ^ 0xd0944565 * nEst
        let i_prn = 0x689e962du32.wrapping_mul(n_col as u32)
            ^ 0xd0944565u32.wrapping_mul(estimated_rows as u32);
        Self {
            n_col,
            n_key_col,
            n_row: 0,
            distinct: vec![0; n_key_col],
            collect_stat4: true,
            max_samples,
            current_n_eq: vec![0; n_col],
            current_n_lt: vec![0; n_col],
            current_n_distinct_lt: vec![0; n_col],
            samples: Vec::with_capacity(max_samples + 1),
            best: (0..n_key_col)
                .map(|i| Stat4AccumSample {
                    key: Vec::new(),
                    n_eq: vec![0; n_col],
                    n_lt: vec![0; n_col],
                    n_distinct_lt: vec![0; n_col],
                    is_periodic: false,
                    i_col: i,
                    i_hash: 0,
                })
                .collect(),
            i_min: 0,
            n_max_eq_zero: 0,
            n_psample: n_psample.max(1),
            current_key: Vec::new(),
            i_prn,
            current_hash: 0,
            stat4_finalized: false,
        }
    }

    /// Build a snapshot of the current row as a sample.
    fn current_sample(&self, i_col: usize, is_periodic: bool) -> Stat4AccumSample {
        Stat4AccumSample {
            key: self.current_key.clone(),
            n_eq: self.current_n_eq.clone(),
            n_lt: self.current_n_lt.clone(),
            n_distinct_lt: self.current_n_distinct_lt.clone(),
            is_periodic,
            i_col,
            i_hash: self.current_hash,
        }
    }

    /// Push the previous best samples for columns >= i_chng into the sample
    /// array. Then update the anEq[] fields of existing samples.
    /// Matches SQLite's samplePushPrevious().
    fn sample_push_previous(&mut self, i_chng: usize) {
        // Push best samples for columns nCol-2 down to iChng.
        // (nCol-2 = n_key_col - 1 = last key column index)
        // Matches SQLite's: for(i=nCol-2; i>=iChng; i--)
        if self.n_col >= 2 && i_chng <= self.n_col - 2 {
            let mut i = self.n_col - 2; // = n_key_col - 1
            loop {
                // Update the best's anEq[i] with the final value from current
                self.best[i].n_eq[i] = self.current_n_eq[i];

                // Only insert if there's room or it's better than the current min
                let dominated = self.samples.len() >= self.max_samples
                    && !sample_is_better(self.n_col, &self.best[i], &self.samples[self.i_min]);

                if !dominated {
                    let best_clone = self.best[i].clone();
                    self.sample_insert(best_clone, i);
                }

                if i == i_chng {
                    break;
                }
                i -= 1;
            }
        }

        // Update the anEq[] fields of already-collected samples.
        // Samples inserted via sampleInsert with nEqZero > 0 have leading zeros
        // in their anEq arrays. Fill those in now with the current values.
        if i_chng < self.n_max_eq_zero {
            for s in self.samples.iter_mut() {
                for j in i_chng..self.n_col {
                    if s.n_eq[j] == 0 {
                        s.n_eq[j] = self.current_n_eq[j];
                    }
                }
            }
            self.n_max_eq_zero = i_chng;
        }
    }

    /// Insert a sample into the samples array. If the array is full, evict
    /// the least desirable sample. `n_eq_zero` leading entries in the sample's
    /// anEq[] are zeroed (to be filled in later by samplePushPrevious).
    /// Matches SQLite's sampleInsert().
    fn sample_insert(&mut self, mut sample: Stat4AccumSample, n_eq_zero: usize) {
        if n_eq_zero > self.n_max_eq_zero {
            self.n_max_eq_zero = n_eq_zero;
        }

        // For non-periodic samples: check if an existing sample shares the same
        // prefix and can be "upgraded" instead of inserting a new sample.
        if !sample.is_periodic {
            let mut upgrade_idx: Option<usize> = None;
            for i in (0..self.samples.len()).rev() {
                if self.samples[i].n_eq[sample.i_col] == 0 {
                    if self.samples[i].is_periodic {
                        return; // Can't upgrade a periodic sample; skip insertion
                    }
                    // This existing sample is for a "deeper" column and shares our prefix
                    if upgrade_idx.is_none()
                        || sample_is_better(
                            self.n_col,
                            &self.samples[i],
                            &self.samples[upgrade_idx.unwrap()],
                        )
                    {
                        upgrade_idx = Some(i);
                    }
                }
            }
            if let Some(idx) = upgrade_idx {
                // Upgrade existing sample to represent our column instead
                let i_col = sample.i_col;
                self.samples[idx].i_col = i_col;
                self.samples[idx].n_eq[i_col] = sample.n_eq[i_col];
                self.find_new_min();
                return;
            }
        }

        // If full, remove the minimum sample to make room
        if self.samples.len() >= self.max_samples {
            // Remove the min sample, shift elements down
            self.samples.remove(self.i_min);
        }

        // Zero out the leading nEqZero entries
        for j in 0..n_eq_zero {
            sample.n_eq[j] = 0;
        }

        // Append the new sample
        self.samples.push(sample);

        // Recalculate the minimum
        self.find_new_min();
    }

    /// Find the index of the least desirable non-periodic sample.
    /// Matches the find_new_min label in SQLite's sampleInsert.
    fn find_new_min(&mut self) {
        if self.samples.len() >= self.max_samples {
            let mut i_min: Option<usize> = None;
            for i in 0..self.samples.len() {
                if self.samples[i].is_periodic {
                    continue;
                }
                if i_min.is_none()
                    || sample_is_better(
                        self.n_col,
                        &self.samples[i_min.unwrap()],
                        &self.samples[i],
                    )
                {
                    i_min = Some(i);
                }
            }
            self.i_min = i_min.unwrap_or(0);
        }
    }

    /// Push a row into the accumulator.
    ///
    /// `i_chng` is the index of the leftmost column that changed:
    /// - 0 means column 0 changed (or first row)
    /// - k means columns 0..k-1 were same, column k changed
    /// - n_key_col means all key columns were the same (only rowid differs)
    ///
    /// Note: `i_chng` is in the range 0..n_col (0..=n_key_col). SQLite asserts
    /// `iChng < nCol`, i.e., i_chng <= n_key_col.
    ///
    /// `key` is the sample record blob (index columns + rowid) for stat4.
    pub fn push(&mut self, i_chng: usize, key: Option<Vec<u8>>) {
        if self.n_row == 0 {
            // First row: initialize anEq to all 1s
            if self.collect_stat4 {
                for j in 0..self.n_col {
                    self.current_n_eq[j] = 1;
                }
            }
        } else {
            // Second and subsequent rows

            // Step 1: Push previous best samples (must happen BEFORE updating
            // anLt/anEq, because samplePushPrevious reads the current anEq).
            if self.collect_stat4 && self.max_samples > 0 {
                self.sample_push_previous(i_chng);
            }

            // Step 2: Update anDLt[], anLt[], anEq[]
            // For i < iChng: just increment anEq[i] (same prefix, one more row)
            if self.collect_stat4 {
                for i in 0..i_chng {
                    self.current_n_eq[i] += 1;
                }
            }
            // For i >= iChng: prefix changed — update nDLt, nLt, reset nEq
            if self.collect_stat4 {
                for i in i_chng..self.n_col {
                    self.current_n_distinct_lt[i] += 1;
                    if self.max_samples > 0 {
                        self.current_n_lt[i] += self.current_n_eq[i];
                    }
                    self.current_n_eq[i] = 1;
                }
            }
        }

        self.n_row += 1;

        // Update stat1 distinct counts (only key columns, not rowid)
        if self.n_row == 1 {
            for d in self.distinct.iter_mut() {
                *d += 1;
            }
        } else {
            for i in i_chng..self.n_key_col {
                self.distinct[i] += 1;
            }
        }

        if self.collect_stat4 && self.max_samples > 0 {
            // Store the current key
            if let Some(k) = key {
                self.current_key = k;
            }

            // Update PRNG hash
            self.i_prn = self.i_prn.wrapping_mul(1103515245).wrapping_add(12345);
            self.current_hash = self.i_prn;

            // Check for periodic sample using nLt of the last column (rowid)
            let n_lt_last = self.current_n_lt[self.n_col - 1];
            if (n_lt_last / self.n_psample) != ((n_lt_last + 1) / self.n_psample) {
                let sample = self.current_sample(0, true);
                self.sample_insert(sample, self.n_col - 1);
            }

            // Update aBest[]: for each key column, check if current is better
            for i in 0..self.n_key_col {
                let current = self.current_sample(i, false);
                if i >= i_chng || sample_is_better_post(self.n_col, &current, &self.best[i]) {
                    self.best[i] = current;
                }
            }
        }
    }

    /// Finalize stat4 collection. Called on first stat_get after the scan.
    /// Matches SQLite's samplePushPrevious(p, 0) called from statGet().
    /// Idempotent: subsequent calls are no-ops.
    pub fn finalize_stat4(&mut self) {
        if !self.collect_stat4 || self.max_samples == 0 || self.stat4_finalized {
            return;
        }
        self.stat4_finalized = true;
        // Push all remaining best samples (equivalent to samplePushPrevious(0))
        self.sample_push_previous(0);
        // Sort samples by nLt[last] (full-key order) for binary search
        self.samples
            .sort_by(|a, b| a.n_lt.last().cmp(&b.n_lt.last()));
    }

    /// Get the number of stat4 samples collected.
    pub fn stat4_sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Get a stat4 sample by index.
    pub fn get_stat4_sample(&self, idx: usize) -> Option<&Stat4AccumSample> {
        self.samples.get(idx)
    }

    /// Get the stat1 string: "total avg1 avg2 ..."
    /// where avgN = ceil(total / distinctN). Uses n_key_col distinct counts.
    pub fn get_stat1(&self) -> String {
        if self.n_row == 0 {
            return String::new();
        }

        let mut parts = vec![self.n_row.to_string()];
        for &d in &self.distinct {
            let avg = if d > 0 {
                self.n_row.div_ceil(d)
            } else {
                self.n_row
            };
            parts.push(avg.to_string());
        }
        parts.join(" ")
    }

}

/// Returns true if sample `a` is "better" than sample `b`.
/// Free function to avoid borrowing `&self` while indexing into the samples array.
/// Matches SQLite's sampleIsBetter().
fn sample_is_better(n_col: usize, a: &Stat4AccumSample, b: &Stat4AccumSample) -> bool {
    let n_eq_a = a.n_eq[a.i_col];
    let n_eq_b = b.n_eq[b.i_col];
    if n_eq_a > n_eq_b {
        return true;
    }
    if n_eq_a == n_eq_b {
        if a.i_col < b.i_col {
            return true;
        }
        if a.i_col == b.i_col {
            return sample_is_better_post(n_col, a, b);
        }
    }
    false
}

/// Tiebreaker comparison for samples on the same column.
/// Matches SQLite's sampleIsBetterPost().
fn sample_is_better_post(n_col: usize, a: &Stat4AccumSample, b: &Stat4AccumSample) -> bool {
    debug_assert_eq!(a.i_col, b.i_col);
    for i in (a.i_col + 1)..n_col {
        if a.n_eq[i] > b.n_eq[i] {
            return true;
        }
        if a.n_eq[i] < b.n_eq[i] {
            return false;
        }
    }
    // Final tiebreak by PRNG hash (matches SQLite's iHash comparison)
    a.i_hash > b.i_hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::numeric::Numeric;
    use crate::translate::collate::CollationSeq;
    use crate::types::ImmutableRecord;

    #[test]
    fn parse_stat_numbers_basic() {
        assert_eq!(parse_stat_numbers("10 5 3 1").unwrap(), vec![10, 5, 3, 1]);
        assert_eq!(parse_stat_numbers("  42\t7 ").unwrap(), vec![42, 7]);
        assert!(parse_stat_numbers("abc 1").is_none());
    }

    /// Build a single-column record blob from an integer value.
    fn make_record_blob(val: i64) -> Vec<u8> {
        let values = [Value::Numeric(Numeric::Integer(val))];
        let record = ImmutableRecord::from_values(values.iter(), 1);
        record.get_payload().to_vec()
    }

    /// Build a minimal IndexStat with synthetic samples for testing.
    /// Creates samples at evenly spaced integer values.
    fn make_test_idx_stats(total_rows: u64, sample_values: &[i64]) -> IndexStat {
        let n = sample_values.len() as u64;
        let rows_per_sample = total_rows / n.max(1);
        let mut samples = Vec::new();
        for (i, &val) in sample_values.iter().enumerate() {
            samples.push(Stat4IndexSample {
                sample: make_record_blob(val),
                n_eq: vec![rows_per_sample],
                n_lt: vec![i as u64 * rows_per_sample],
                n_distinct_lt: vec![i as u64],
            });
        }
        IndexStat {
            total_rows: Some(total_rows),
            avg_rows_per_distinct_prefix: vec![rows_per_sample],
            samples,
            avg_eq: vec![rows_per_sample as f64],
        }
    }

    #[test]
    fn stat4_key_stats_exact_match() {
        // 1000 rows, samples at 100, 300, 500, 700, 900
        let stats = make_test_idx_stats(1000, &[100, 300, 500, 700, 900]);
        let probe = Value::Numeric(Numeric::Integer(500));
        let result = stat4_key_stats(&stats, &probe, CollationSeq::Binary, false);
        assert!(result.is_some());
        let (n_lt, n_eq) = result.unwrap();
        // Sample at index 2: n_lt = 2*200 = 400, n_eq = 200
        assert_eq!(n_lt, 400);
        assert_eq!(n_eq, 200);
    }

    #[test]
    fn stat4_key_stats_between_samples() {
        let stats = make_test_idx_stats(1000, &[100, 300, 500, 700, 900]);
        // Probe at 400 — between samples[1] (300) and samples[2] (500)
        let probe = Value::Numeric(Numeric::Integer(400));
        let result_low = stat4_key_stats(&stats, &probe, CollationSeq::Binary, false);
        let result_high = stat4_key_stats(&stats, &probe, CollationSeq::Binary, true);
        assert!(result_low.is_some());
        assert!(result_high.is_some());
        let (n_lt_low, _) = result_low.unwrap();
        let (n_lt_high, _) = result_high.unwrap();
        // round_up=false gives lower estimate, round_up=true gives higher
        assert!(n_lt_low <= n_lt_high);
    }

    #[test]
    fn stat4_key_stats_past_all_samples() {
        let stats = make_test_idx_stats(1000, &[100, 300, 500]);
        let probe = Value::Numeric(Numeric::Integer(999));
        let result = stat4_key_stats(&stats, &probe, CollationSeq::Binary, true);
        assert!(result.is_some());
        let (n_lt, _) = result.unwrap();
        // Past all samples — interpolated near total_rows
        assert!(n_lt > 0);
    }

    #[test]
    fn stat4_key_stats_before_all_samples() {
        let stats = make_test_idx_stats(1000, &[100, 300, 500]);
        let probe = Value::Numeric(Numeric::Integer(1));
        let result = stat4_key_stats(&stats, &probe, CollationSeq::Binary, false);
        assert!(result.is_some());
        let (n_lt, _) = result.unwrap();
        // Before all samples — near 0
        assert!(n_lt < 200);
    }

    #[test]
    fn stat4_key_stats_empty_samples() {
        let stats = IndexStat::default();
        let probe = Value::Numeric(Numeric::Integer(42));
        assert!(stat4_key_stats(&stats, &probe, CollationSeq::Binary, false).is_none());
    }

    #[test]
    fn stat4_range_selectivity_closed_range() {
        let stats = make_test_idx_stats(1000, &[100, 300, 500, 700, 900]);
        let lower = Value::Numeric(Numeric::Integer(200));
        let upper = Value::Numeric(Numeric::Integer(800));
        let sel = stat4_range_selectivity(
            &stats,
            Some((&lower, false)), // >= 200
            Some((&upper, true)),  // < 800
            CollationSeq::Binary,
        );
        assert!(sel.is_some());
        let s = sel.unwrap();
        // Should be roughly (800-200)/1000 ≈ 0.6, but actual depends on sample interpolation
        assert!(s > 0.1);
        assert!(s < 1.0);
    }

    #[test]
    fn stat4_range_selectivity_open_lower() {
        let stats = make_test_idx_stats(1000, &[100, 300, 500, 700, 900]);
        let lower = Value::Numeric(Numeric::Integer(500));
        let sel = stat4_range_selectivity(
            &stats,
            Some((&lower, true)), // > 500
            None,                 // no upper
            CollationSeq::Binary,
        );
        assert!(sel.is_some());
        let s = sel.unwrap();
        // > 500: roughly 40% of rows
        assert!(s > 0.1);
        assert!(s < 0.9);
    }

    #[test]
    fn stat4_range_selectivity_no_bounds() {
        let stats = make_test_idx_stats(1000, &[100, 300, 500]);
        assert!(stat4_range_selectivity(&stats, None, None, CollationSeq::Binary).is_none());
    }

    #[test]
    fn stat4_range_selectivity_empty_stats() {
        let stats = IndexStat::default();
        let v = Value::Numeric(Numeric::Integer(42));
        assert!(stat4_range_selectivity(
            &stats,
            Some((&v, false)),
            None,
            CollationSeq::Binary
        )
        .is_none());
    }
}
