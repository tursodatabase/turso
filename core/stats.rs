use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
use arc_swap::ArcSwap;
use rustc_hash::FxHashMap as HashMap;

use crate::schema::Schema;
use crate::translate::emitter::TransactionMode;
use crate::types::IOResult;
use crate::util::normalize_ident;
use crate::{Connection, Result, Statement, TransactionState, Value};
pub const STATS_TABLE: &str = "sqlite_stat1";
const STATS_QUERY: &str = "SELECT tbl, idx, stat FROM sqlite_stat1";

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

/// Atomically swappable ANALYZE statistics shared across every clone of a schema.
///
/// `epoch` advances on every change. A connection records the epoch it last
/// planned against and bumps its `prepare_context_generation` when the epoch
/// moves, forcing cached statements with stale or empty stats to recompile.
#[derive(Debug)]
pub struct SharedAnalyzeStats {
    stats: ArcSwap<AnalyzeStats>,
    epoch: AtomicU64,
}

impl Default for SharedAnalyzeStats {
    fn default() -> Self {
        Self {
            stats: ArcSwap::from_pointee(AnalyzeStats::default()),
            epoch: AtomicU64::new(0),
        }
    }
}

impl SharedAnalyzeStats {
    /// Load a cheap, point-in-time snapshot of the current statistics. Hold the
    /// returned `Arc` for the duration of a planning pass so the view is stable.
    pub fn snapshot(&self) -> Arc<AnalyzeStats> {
        self.stats.load_full()
    }

    /// Current change epoch. See the struct docs for how it drives replanning.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Whether no statistics have been loaded yet.
    pub fn is_empty(&self) -> bool {
        self.stats.load().tables.is_empty()
    }

    /// Replace the statistics wholesale and advance the epoch.
    pub fn store(&self, stats: AnalyzeStats) {
        self.stats.store(Arc::new(stats));
        self.epoch.fetch_add(1, Ordering::Release);
    }

    /// Drop all statistics for a table (e.g. on DROP TABLE). Copy-on-write: the
    /// statistics map is small relative to the schema, and DDL is rare.
    pub fn remove_table(&self, table_name: &str) {
        let mut next = (**self.stats.load()).clone();
        let before = next.tables.len();
        next.remove_table(table_name);
        if next.tables.len() != before {
            self.store(next);
        }
    }

    /// Drop statistics for a single index (e.g. on DROP INDEX).
    pub fn remove_index(&self, table_name: &str, index_name: &str) {
        let mut next = (**self.stats.load()).clone();
        next.remove_index(table_name, index_name);
        self.store(next);
    }
}

/// Read sqlite_stat1 contents into an AnalyzeStats map without mutating schema.
///
/// Only regular B-tree tables and indexes are considered. Virtual and ephemeral
/// tables are ignored.
pub fn gather_sqlite_stat1(
    conn: &Arc<Connection>,
    schema: &Schema,
    mv_tx: Option<(u64, TransactionMode)>,
) -> Result<AnalyzeStats> {
    let mut stats = AnalyzeStats::default();
    let mut stmt = conn.prepare(STATS_QUERY)?;
    stmt.set_mv_tx(mv_tx);
    load_sqlite_stat1_from_stmt(stmt, schema, &mut stats)?;
    Ok(stats)
}

/// Called at the start of statement compilation. Does two things:
///
/// 1. Detects when the shared ANALYZE stats have been refreshed (by any
///    connection) since this connection last planned, and bumps the prepare
///    context generation so cached statements recompile against the new stats.
/// 2. Lazily loads stats from `sqlite_stat1` into the shared cell when they are
///    missing — needed under MVCC, where the user tables (including
///    `sqlite_stat1`) only reach this connection's schema after the
///    connect()-time refresh has already run and loaded nothing.
///
/// Both steps mutate the stats via the [`SharedAnalyzeStats`] cell, so neither
/// deep-clones the (potentially huge) schema.
pub fn maybe_lazy_load_analyze_stats(conn: &Arc<Connection>) {
    // Nested sub-programs (e.g. trigger bodies, the internal stats query below)
    // must never touch the prepare context; the parent compilation owns it.
    if conn.is_nested_stmt() || !conn.is_db_initialized() {
        return;
    }

    // Adopt any stats refresh published by ourselves or another
    // connection. `swap` records the epoch we are now planning against.
    let cell_epoch = conn.schema.read().analyze_stats.epoch();
    if conn
        .analyze_stats_seen_epoch
        .swap(cell_epoch, Ordering::AcqRel)
        != cell_epoch
    {
        conn.bump_prepare_context_generation();
    }

    // Load stats from sqlite_stat1 if we have not already tried for this
    // generation. Guarding on the generation also stops the internal
    // STATS_QUERY prepare below from re-entering this load.
    let generation = conn.prepare_context_generation();
    if conn
        .analyze_stats_attempt_generation
        .load(Ordering::Acquire)
        == generation
    {
        return;
    }
    // Transient states: skip without marking so a later prepare retries.
    if conn.schema_reparse_in_progress()
        || conn.is_mvcc_bootstrap_connection()
        || !matches!(conn.get_tx_state(), TransactionState::None)
        || conn.get_mv_tx().is_some()
    {
        return;
    }
    let schema = conn.schema.read().clone();
    if !schema.analyze_stats.is_empty() || schema.get_btree_table(STATS_TABLE).is_none() {
        // Nothing to load for this schema; don't re-check until it changes.
        conn.analyze_stats_attempt_generation
            .store(generation, Ordering::Release);
        return;
    }
    // Mark before running the internal query so this prepare's nested
    // statement cannot re-enter, and errors don't retry on every prepare.
    conn.analyze_stats_attempt_generation
        .store(generation, Ordering::Release);
    let Ok(stats) = gather_sqlite_stat1(conn, &schema, None) else {
        return;
    };
    if stats.tables.is_empty() {
        return;
    }
    // Publish into the shared cell. Every connection holding any clone of this
    // schema lineage observes the stats; the epoch bump (and our own generation
    // bump) forces statements planned without stats to recompile.
    schema.analyze_stats.store(stats);
    conn.analyze_stats_seen_epoch
        .store(schema.analyze_stats.epoch(), Ordering::Release);
    conn.bump_prepare_context_generation();
}

/// Best-effort refresh analyze_stats on the connection's schema.
pub fn refresh_analyze_stats(conn: &Arc<Connection>) {
    if !conn.is_db_initialized() || conn.is_nested_stmt() {
        return;
    }
    if matches!(conn.get_tx_state(), TransactionState::Write { .. }) {
        return;
    }

    // Need a snapshot of the current schema to validate tables/indexes.
    let schema_snapshot = { conn.schema.read().clone() };
    if schema_snapshot.get_btree_table(STATS_TABLE).is_none() {
        return;
    }

    let mv_tx = conn.get_mv_tx();
    if let Ok(stats) = gather_sqlite_stat1(conn, &schema_snapshot, mv_tx) {
        schema_snapshot.analyze_stats.store(stats);
    }
}

/// Carries the in-progress `sqlite_stat1` scan across IO yields for
/// [`refresh_analyze_stats_nonblock`].
#[derive(Default)]
pub enum RefreshAnalyzeStatsState {
    #[default]
    Start,
    Running {
        stmt: Box<Statement>,
        schema_snapshot: Arc<Schema>,
        stats: AnalyzeStats,
    },
}

/// Non-blocking variant of [`refresh_analyze_stats`]: best-effort refresh of
/// the connection's in-memory ANALYZE stats from `sqlite_stat1`, yielding IO
/// via the supplied state instead of pumping it. Errors are swallowed (matches
/// the blocking variant's best-effort contract).
pub fn refresh_analyze_stats_nonblock(
    conn: &Arc<Connection>,
    st: &mut RefreshAnalyzeStatsState,
) -> Result<IOResult<()>> {
    loop {
        match st {
            RefreshAnalyzeStatsState::Start => {
                if !conn.is_db_initialized() || conn.is_nested_stmt() {
                    return Ok(IOResult::Done(()));
                }
                if matches!(conn.get_tx_state(), TransactionState::Write { .. }) {
                    return Ok(IOResult::Done(()));
                }
                let schema_snapshot = { conn.schema.read().clone() };
                if schema_snapshot.get_btree_table(STATS_TABLE).is_none() {
                    return Ok(IOResult::Done(()));
                }
                let mv_tx = conn.get_mv_tx();
                let mut stmt = conn.prepare(STATS_QUERY)?;
                stmt.set_mv_tx(mv_tx);
                *st = RefreshAnalyzeStatsState::Running {
                    stmt: Box::new(stmt),
                    schema_snapshot,
                    stats: AnalyzeStats::default(),
                };
            }
            RefreshAnalyzeStatsState::Running {
                stmt,
                schema_snapshot,
                stats,
            } => {
                let scan = load_sqlite_stat1_rows_nonblock(stmt, schema_snapshot, stats);
                match scan {
                    Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
                    Ok(IOResult::Done(())) => {
                        let stats = std::mem::take(stats);
                        // Store into the shared cell captured at Start. During
                        // schema reparse this snapshot shares its stats cell with
                        // the schema about to be installed; otherwise the lazy
                        // loader will reload on the next prepare.
                        schema_snapshot.analyze_stats.store(stats);
                        *st = RefreshAnalyzeStatsState::Start;
                        return Ok(IOResult::Done(()));
                    }
                    Err(e) => {
                        tracing::warn!("Failed to refresh analyze stats: {e}");
                        *st = RefreshAnalyzeStatsState::Start;
                        return Ok(IOResult::Done(()));
                    }
                }
            }
        }
    }
}

/// Non-blocking row scan shared by [`refresh_analyze_stats_nonblock`]. Steps the
/// prepared `sqlite_stat1` statement, accumulating into `stats`.
fn load_sqlite_stat1_rows_nonblock(
    stmt: &mut Statement,
    schema: &Schema,
    stats: &mut AnalyzeStats,
) -> Result<crate::types::IOResult<()>> {
    crate::return_if_io!(
        stmt.run_with_row_callback_nonblock(|row| { load_sqlite_stat1_row(row, schema, stats) })
    );
    Ok(crate::types::IOResult::Done(()))
}

/// Apply a single `sqlite_stat1` row to the accumulating [`AnalyzeStats`].
/// Shared by the blocking and non-blocking scanners.
fn load_sqlite_stat1_row(
    row: &crate::vdbe::Row,
    schema: &Schema,
    stats: &mut AnalyzeStats,
) -> Result<()> {
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
        _ => return Ok(()),
    };

    // Skip if table is not a regular B-tree.
    if schema.get_btree_table(table_name).is_none() {
        return Ok(());
    }
    let Some(numbers) = parse_stat_numbers(stat) else {
        return Ok(());
    };
    if numbers.is_empty() {
        return Ok(());
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
}

fn load_sqlite_stat1_from_stmt(
    mut stmt: Statement,
    schema: &Schema,
    stats: &mut AnalyzeStats,
) -> Result<()> {
    stmt.run_with_row_callback(|row| load_sqlite_stat1_row(row, schema, stats))?;
    Ok(())
}

fn parse_stat_numbers(stat: &str) -> Option<Vec<u64>> {
    stat.split_whitespace()
        .map(|s| s.parse::<u64>().ok())
        .collect()
}

/// Statistics accumulator for ANALYZE.
#[derive(Debug, Clone)]
pub struct StatAccum {
    /// Number of columns in the index (not including rowid)
    pub n_col: usize,
    /// Total number of rows seen
    pub n_row: u64,
    /// Distinct counts for each column prefix.
    /// distinct[i] = number of distinct values for columns 0..=i
    pub distinct: Vec<u64>,
}

impl StatAccum {
    pub fn new(n_col: usize) -> Self {
        Self {
            n_col,
            n_row: 0,
            distinct: vec![0; n_col],
        }
    }

    /// Push a row, indicating which column (0-indexed) is the first to differ
    /// from the previous row. If this is the first row, pass 0.
    ///
    /// i_chng is the index of the leftmost column that changed:
    /// - 0 means column 0 changed (or first row)
    /// - 1 means columns 0 was same, column 1 changed
    /// - n_col means all columns were the same (duplicate row)
    pub fn push(&mut self, i_chng: usize) {
        self.n_row += 1;
        // Increment distinct counts for columns i_chng and onwards
        // because if column i changed, then prefixes (0..=i), (0..=i+1), etc. all have a new distinct value
        for i in i_chng..self.n_col {
            self.distinct[i] += 1;
        }
    }

    /// Get the stat1 string: "total avg1 avg2 ..."
    /// where avgN = ceil(total / distinctN)
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

    /// Serialize to bytes for storage in a blob register.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 8 + 8 * self.n_col);
        bytes.extend_from_slice(&(self.n_col as u64).to_le_bytes());
        bytes.extend_from_slice(&self.n_row.to_le_bytes());
        for &d in &self.distinct {
            bytes.extend_from_slice(&d.to_le_bytes());
        }
        bytes
    }

    /// Deserialize from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 16 {
            return None;
        }
        let n_col = u64::from_le_bytes(bytes[0..8].try_into().ok()?) as usize;
        let n_row = u64::from_le_bytes(bytes[8..16].try_into().ok()?);

        if bytes.len() < 16 + 8 * n_col {
            return None;
        }
        let mut distinct = Vec::with_capacity(n_col);
        for i in 0..n_col {
            let start = 16 + i * 8;
            let d = u64::from_le_bytes(bytes[start..start + 8].try_into().ok()?);
            distinct.push(d);
        }
        Some(Self {
            n_col,
            n_row,
            distinct,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::parse_stat_numbers;

    #[test]
    fn parse_stat_numbers_basic() {
        assert_eq!(parse_stat_numbers("10 5 3 1").unwrap(), vec![10, 5, 3, 1]);
        assert_eq!(parse_stat_numbers("  42\t7 ").unwrap(), vec![42, 7]);
        assert!(parse_stat_numbers("abc 1").is_none());
    }
}
