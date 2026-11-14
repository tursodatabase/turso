use crate::namespace::NamespaceId;
use crate::sync::Arc;
use crate::types::IOResultOr;
use rustc_hash::FxHashMap as HashMap;

use crate::alloc::TursoVecExt;
use crate::schema::Schema;
use crate::statement::StatementOrigin;
use crate::types::IOResult;
use crate::util::normalize_ident;
use crate::{Connection, Result, Statement, TransactionState, Value};
pub const STATS_TABLE: &str = "sqlite_stat1";
const STATS_QUERY: &str = "SELECT tbl, idx, stat FROM sqlite_stat1";

/// Statistics produced by ANALYZE for a single index.
#[derive(Clone, Debug, Default, PartialEq)]
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
#[derive(Clone, Debug, Default, PartialEq)]
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
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AnalyzeStats {
    /// Per-table statistics keyed by namespace and normalized table name.
    pub tables: HashMap<(NamespaceId, String), TableStat>,
}

impl AnalyzeStats {
    pub fn needs_refresh(&self) -> bool {
        self.tables.is_empty()
    }
    /// Get the statistics for a table, if present.
    pub fn table_stats(&self, namespace: NamespaceId, table_name: &str) -> Option<&TableStat> {
        let table_name = normalize_ident(table_name);
        self.tables.get(&(namespace, table_name))
    }

    /// Get or create the statistics bucket for a table.
    pub fn table_stats_mut(&mut self, namespace: NamespaceId, table_name: &str) -> &mut TableStat {
        let table_name = normalize_ident(table_name);
        self.tables.entry((namespace, table_name)).or_default()
    }

    /// Remove all statistics for a table.
    pub fn remove_table(&mut self, namespace: NamespaceId, table_name: &str) {
        let table_name = normalize_ident(table_name);
        self.tables.remove(&(namespace, table_name));
    }

    /// Remove statistics for a specific index on a table.
    pub fn remove_index(&mut self, namespace: NamespaceId, table_name: &str, index_name: &str) {
        let table_name = normalize_ident(table_name);
        let index_name = normalize_ident(index_name);
        if let Some(table_stats) = self.tables.get_mut(&(namespace, table_name)) {
            table_stats.index_stats.remove(&index_name);
        }
    }
}

/// Best-effort refresh of the connection's in-memory ANALYZE stats.
///
/// Blocking form of [`refresh_analyze_stats_nonblock`]: the same state machine,
/// driven to completion here by pumping IO. That is only correct when whatever
/// the `sqlite_stat1` scan waits for can make progress without the caller's
/// help (real I/O, or another thread). A host that schedules connections
/// cooperatively on one thread must drive the non-blocking variant itself,
/// because an explicit yield is a request to run *other* connections, which
/// `io.step()` never does; see `Database::connect_async`.
pub fn refresh_analyze_stats(conn: &Arc<Connection>) {
    let io = conn.db.io.clone();
    let mut state = RefreshAnalyzeStatsState::default();
    loop {
        let pending = match refresh_analyze_stats_nonblock(conn, &mut state) {
            Ok(IOResult::Done(())) => return,
            Ok(IOResult::IO(pending)) => pending,
            Err(e) => {
                tracing::warn!("Failed to refresh analyze stats: {e}");
                return;
            }
        };
        // An explicit yield carries nothing to wait for; give the IO one turn,
        // as the blocking runner always did, and step the scan again.
        let advanced = if pending.is_explicit_yield() {
            io.step()
        } else {
            pending.wait(io.as_ref())
        };
        if let Err(e) = advanced {
            tracing::warn!("Failed to refresh analyze stats: {e}");
            return;
        }
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
) -> IOResultOr<()> {
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
                if schema_snapshot
                    .get_btree_table(NamespaceId::ROOT, STATS_TABLE)
                    .is_none()
                {
                    return Ok(IOResult::Done(()));
                }
                let mv_tx = conn.get_mv_tx();
                let mut stmt =
                    conn.prepare_with_origin(STATS_QUERY, StatementOrigin::EngineMetadataRead)?;
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
                        if let Err(e) = install_analyze_stats(conn, stats) {
                            tracing::warn!("Failed to refresh analyze stats: {e}");
                        }
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

/// Store freshly gathered stats in the connection's schema and, when the
/// connection already shares the database schema object, in the shared
/// schema too.
///
/// In MVCC mode the commit that wrote `sqlite_stat1` publishes the connection's
/// schema to the shared schema before the stats are gathered. If only the
/// connection copy were updated, the two copies would differ while having the
/// same version, and the next statement would adopt the shared copy and lose
/// the new stats.
///
/// When the connection shares the database schema object, the stats go into
/// the shared schema first and the connection is then pointed at the shared
/// schema, instead of deep-cloning the whole schema into the connection just
/// to set the stats. In that case the shared schema is only cloned when the
/// new stats actually differ from the ones it already has, so a connection
/// opened after ANALYZE normally pays nothing.
///
/// Same schema version is not enough to point a connection at the shared
/// schema: a reparse or a sync checkpoint can install a private schema at an
/// unchanged version (see `force_reparse_schema_without_publish`), and
/// pointing such a connection at the shared schema would discard it. A
/// connection whose schema is a private copy keeps the old per-connection
/// install below.
///
/// The connection schema lock and the shared schema lock are never held at
/// the same time. The connection is only pointed at the shared schema while
/// it still holds the same schema object compared against; if its schema
/// changed in the meantime, the stats are installed in the connection's own
/// copy instead.
///
/// No prepare-generation bump is needed here: the schema the connection ends
/// up with differs from its old one only in stats, which the old
/// per-connection install also left unbumped.
///
/// In the reparse path this refresh runs before finalize replaces the
/// connection schema with the freshly built one, which carries no stats yet;
/// that end state (connection without stats, shared with stats) is
/// pre-existing and unchanged by this fix.
fn install_analyze_stats(conn: &Arc<Connection>, stats: AnalyzeStats) -> Result<()> {
    let connection_schema = conn.schema.read().clone();
    let mut shared = conn.db.schema.lock();
    if Arc::ptr_eq(&connection_schema, &shared) {
        if shared.analyze_stats != stats {
            Schema::try_make_mut(&mut shared)?.analyze_stats = stats.clone();
        }
        let shared_schema = shared.clone();
        drop(shared);
        let mut current = conn.schema.write();
        if Arc::ptr_eq(&*current, &connection_schema) {
            *current = shared_schema;
        } else {
            Schema::try_make_mut(&mut current)?.analyze_stats = stats;
        }
        return Ok(());
    }
    // The snapshot is not used below; drop it so the per-connection install
    // can still mutate a solely-owned schema in place instead of cloning it.
    drop(connection_schema);
    drop(shared);
    let schema_version = conn.with_schema_mut(|schema| {
        schema.analyze_stats = stats.clone();
        schema.schema_version
    })?;
    let mut shared = conn.db.schema.lock();
    if shared.schema_version == schema_version {
        Schema::try_make_mut(&mut shared)?.analyze_stats = stats;
    }
    Ok(())
}

/// Non-blocking row scan shared by [`refresh_analyze_stats_nonblock`]. Steps the
/// prepared `sqlite_stat1` statement, accumulating into `stats`.
fn load_sqlite_stat1_rows_nonblock(
    stmt: &mut Statement,
    schema: &Schema,
    stats: &mut AnalyzeStats,
) -> crate::types::IOResultOr<()> {
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
    let (namespace, table_name) = schema.demangle_name(row.get::<&str>(0)?);
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
    if schema.get_btree_table(namespace, table_name).is_none() {
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
            stats.table_stats_mut(namespace, table_name).row_count = Some(total_rows);
        }
        return Ok(());
    }

    // Index-level entry: only keep if the index exists on this table.
    let idx_name = normalize_ident(idx_name.unwrap());
    if schema.get_index(namespace, table_name, &idx_name).is_none() {
        return Ok(());
    }

    let total_rows = numbers.first().copied();
    {
        let idx_stats = stats
            .table_stats_mut(namespace, table_name)
            .index_stats_mut(&idx_name);
        idx_stats.total_rows = total_rows;
        idx_stats.avg_rows_per_distinct_prefix = numbers.iter().skip(1).copied().collect();
    }

    // If we didn't see a table-level row yet, seed row_count from index stats.
    if let Some(total_rows) = total_rows {
        let table_stats = stats.table_stats_mut(namespace, table_name);
        if table_stats.row_count.is_none() {
            table_stats.row_count = Some(total_rows);
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
    pub fn to_bytes(&self) -> crate::ValueBlob {
        let mut bytes =
            <crate::ValueBlob as TursoVecExt<u8>>::with_capacity(8 + 8 + 8 * self.n_col);
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
    use super::{parse_stat_numbers, AnalyzeStats, NamespaceId};
    use crate::database::{ConnectOpts, Database, DatabaseOpts};
    use crate::dialect::EngineDialect;
    use crate::security::roles::SessionRole;
    use crate::sync::Arc;
    use crate::{OpenFlags, PlatformIO, IO};

    #[test]
    fn parse_stat_numbers_basic() {
        assert_eq!(parse_stat_numbers("10 5 3 1").unwrap(), vec![10, 5, 3, 1]);
        assert_eq!(parse_stat_numbers("  42\t7 ").unwrap(), vec![42, 7]);
        assert!(parse_stat_numbers("abc 1").is_none());
    }

    /// Open a fresh temporary database for the stats tests below.
    fn open_db() -> (tempfile::TempDir, Arc<Database>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stats.db");
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
            Arc::new(EngineDialect),
        )
        .unwrap();
        (dir, db)
    }

    /// Create table `t` (2 rows, 2 indexes) and ANALYZE it on a fresh
    /// connection, which is dropped on return.
    fn create_table_and_analyze(db: &Arc<Database>) {
        let setup = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        setup
            .execute(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c INTEGER);
                 CREATE INDEX t_a ON t(a, c);
                 CREATE INDEX t_b ON t(b);
                 INSERT INTO t (a, b, c) VALUES ('x', 'y', 1), ('z', 'w', 2);
                 ANALYZE",
            )
            .unwrap();
    }

    /// The stats the tests expect for table `t` after ANALYZE on its 2 rows.
    fn assert_table_t_stats(stats: &AnalyzeStats) {
        assert_eq!(stats.tables.len(), 1, "only table t should have stats");
        let t = stats
            .table_stats(NamespaceId::ROOT, "t")
            .expect("table t must have stats");
        assert_eq!(t.row_count, Some(2));
        let t_a = t.index_stats.get("t_a").expect("index t_a must have stats");
        assert_eq!(t_a.total_rows, Some(2));
        let t_b = t.index_stats.get("t_b").expect("index t_b must have stats");
        assert_eq!(t_b.total_rows, Some(2));
    }

    /// Regression test for #11581: once a database has `sqlite_stat1`, opening
    /// a new connection must not deep-clone the whole schema just to install
    /// the ANALYZE stats. Every connection keeps sharing the database schema.
    #[test]
    fn new_connections_share_schema_when_stat1_exists() {
        let (_dir, db) = open_db();
        create_table_and_analyze(&db);
        let mut connections = Vec::new();
        for _ in 0..5 {
            let conn = db
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap();
            assert!(
                Arc::ptr_eq(&conn.schema.read(), &db.schema.lock()),
                "new connection must share the database schema instead of deep-cloning it"
            );
            connections.push(conn);
        }
        assert_eq!(connections.len(), 5);
        assert_table_t_stats(&db.schema.lock().analyze_stats);
    }

    /// An existing connection that runs ANALYZE itself ends up sharing the
    /// database schema and carrying the correct stats (#11581).
    #[test]
    fn existing_connection_shares_schema_after_analyze() {
        let (_dir, db) = open_db();
        let setup = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        setup
            .execute(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c INTEGER);
                 CREATE INDEX t_a ON t(a, c);
                 CREATE INDEX t_b ON t(b);
                 INSERT INTO t (a, b, c) VALUES ('x', 'y', 1), ('z', 'w', 2);",
            )
            .unwrap();
        drop(setup);
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("ANALYZE").unwrap();
        assert!(
            Arc::ptr_eq(&conn.schema.read(), &db.schema.lock()),
            "connection that ran ANALYZE must share the database schema"
        );
        assert_table_t_stats(&conn.schema.read().analyze_stats);
        assert_table_t_stats(&db.schema.lock().analyze_stats);
    }

    /// When the stats actually changed, the shared schema is updated and the
    /// connection is pointed at the new shared schema (#11581).
    #[test]
    fn analyze_after_more_rows_updates_shared_stats() {
        let (_dir, db) = open_db();
        create_table_and_analyze(&db);
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute(
            "INSERT INTO t (a, b, c) VALUES ('p', 'q', 3), ('r', 's', 4), ('u', 'v', 5);
             ANALYZE",
        )
        .unwrap();
        assert!(
            Arc::ptr_eq(&conn.schema.read(), &db.schema.lock()),
            "connection must share the database schema after re-ANALYZE"
        );
        let shared = db.schema.lock();
        assert_eq!(
            shared
                .analyze_stats
                .table_stats(NamespaceId::ROOT, "t")
                .unwrap()
                .row_count,
            Some(5)
        );
        drop(shared);
        let conn2 = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        assert!(Arc::ptr_eq(&conn2.schema.read(), &db.schema.lock()));
        assert_eq!(
            conn2
                .schema
                .read()
                .analyze_stats
                .table_stats(NamespaceId::ROOT, "t")
                .unwrap()
                .row_count,
            Some(5)
        );
    }

    /// A connection holding a private copy of the schema at the same version
    /// (installed here by a reparse without publishing, the shape a sync
    /// checkpoint leaves behind) is not pointed at the shared schema by a
    /// stats refresh: it keeps its own schema and still gets the stats
    /// (#11581).
    #[test]
    fn private_same_version_schema_survives_analyze() {
        let (_dir, db) = open_db();
        create_table_and_analyze(&db);
        let a = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        a.force_reparse_schema_without_publish().unwrap();
        assert!(
            !Arc::ptr_eq(&a.schema.read(), &db.schema.lock()),
            "test setup must leave a private schema on the connection"
        );
        a.execute("ANALYZE").unwrap();
        assert!(
            !Arc::ptr_eq(&a.schema.read(), &db.schema.lock()),
            "refresh must not discard a private same-version schema"
        );
        assert_table_t_stats(&a.schema.read().analyze_stats);
    }

    /// Control case for #11581: without `sqlite_stat1` there is nothing to
    /// install, so new connections trivially keep sharing the database schema.
    #[test]
    fn connections_share_schema_without_stat1() {
        let (_dir, db) = open_db();
        let setup = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        setup
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        drop(setup);
        let mut connections = Vec::new();
        for _ in 0..5 {
            let conn = db
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap();
            assert!(
                Arc::ptr_eq(&conn.schema.read(), &db.schema.lock()),
                "new connection must share the database schema"
            );
            connections.push(conn);
        }
        assert_eq!(connections.len(), 5);
    }
}
