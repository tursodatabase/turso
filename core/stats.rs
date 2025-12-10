use std::collections::HashMap;
use std::sync::Arc;

use crate::schema::Schema;
use crate::translate::emitter::TransactionMode;
use crate::util::normalize_ident;
use crate::{Connection, Result, Statement, StepResult, TransactionState, Value};
pub const STATS_TABLE: &str = "sqlite_stat1";
const STATS_QUERY: &str = "SELECT tbl, idx, stat FROM sqlite_stat1";

/// Statistics produced by ANALYZE for a single index.
#[derive(Clone, Debug, Default)]
pub struct IndexStat {
    /// Estimated total number of rows in the table/index when the stat was collected.
    pub total_rows: Option<u64>,
    /// Estimated number of distinct keys for each leftmost prefix of the index
    /// columns. The entry at position `i` is the distinct count for the first
    /// `i + 1` columns of the index.
    pub distinct_per_prefix: Vec<u64>,
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
        conn.with_schema_mut(|schema| {
            schema.analyze_stats = stats;
        });
    }
}

fn load_sqlite_stat1_from_stmt(
    mut stmt: Statement,
    schema: &Schema,
    stats: &mut AnalyzeStats,
) -> Result<()> {
    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().expect("row should be present");
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
                    _ => continue,
                };

                // Skip if table is not a regular B-tree.
                if schema.get_btree_table(table_name).is_none() {
                    continue;
                }
                let Some(numbers) = parse_stat_numbers(stat) else {
                    continue;
                };
                if numbers.is_empty() {
                    continue;
                }
                if idx_name.is_none() {
                    if let Some(total_rows) = numbers.first().copied() {
                        stats.table_stats_mut(table_name).row_count = Some(total_rows);
                    }
                    continue;
                }

                // Index-level entry: only keep if the index exists on this table.
                let idx_name = normalize_ident(idx_name.unwrap());
                if schema.get_index(table_name, &idx_name).is_none() {
                    continue;
                }

                let total_rows = numbers.first().copied();
                {
                    let idx_stats = stats.table_stats_mut(table_name).index_stats_mut(&idx_name);
                    idx_stats.total_rows = total_rows;
                    idx_stats.distinct_per_prefix = numbers.iter().skip(1).copied().collect();
                }

                // If we didn't see a table-level row yet, seed row_count from index stats.
                if let Some(total_rows) = total_rows {
                    let table_stats = stats.table_stats_mut(table_name);
                    if table_stats.row_count.is_none() {
                        table_stats.row_count = Some(total_rows);
                    }
                }
            }
            StepResult::Done => break,
            StepResult::IO => {
                stmt.run_once()?;
            }
            StepResult::Interrupt => {
                return Err(crate::LimboError::InternalError("interrupted".to_string()))
            }
            StepResult::Busy => return Err(crate::LimboError::Busy),
        }
    }
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
