use std::fmt::Display;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::panic::UnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bitmaps::Bitmap;
use garde::Validate;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sql_generation::generation::GenerationContext;
use sql_generation::model::query::transaction::Rollback;
use sql_generation::model::table::Table;
use tracing::trace;
use turso_core::Database;

use crate::generation::Shadow;
use crate::model::Query;
use crate::profiles::Profile;
use crate::runner::SimIO;
use crate::runner::io::SimulatorIO;
use crate::runner::memory::io::MemorySimIO;

use super::cli::SimulatorCLI;

#[derive(Debug, Copy, Clone)]
pub(crate) enum SimulationType {
    Default,
    Doublecheck,
    Differential,
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum SimulationPhase {
    Test,
    Shrink,
}

#[derive(Debug, Clone)]
pub struct TransactionTables {
    current_tables: Vec<Table>,
    pending_changes: Vec<Query>,
}

impl TransactionTables {
    pub fn new(tables: Vec<Table>) -> Self {
        Self {
            current_tables: tables,
            pending_changes: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct ShadowTables<'a> {
    commited_tables: &'a Vec<Table>,
    transaction_tables: Option<&'a TransactionTables>,
}

#[derive(Debug)]
pub struct ShadowTablesMut<'a> {
    commited_tables: &'a mut Vec<Table>,
    transaction_tables: &'a mut Option<TransactionTables>,
}

impl<'a> ShadowTables<'a> {
    fn tables(&self) -> &'a Vec<Table> {
        self.transaction_tables
            .map_or(self.commited_tables, |v| &v.current_tables)
    }
}

impl<'a> Deref for ShadowTables<'a> {
    type Target = Vec<Table>;

    fn deref(&self) -> &Self::Target {
        self.tables()
    }
}

impl<'a, 'b> ShadowTablesMut<'a>
where
    'a: 'b,
{
    fn tables(&'a self) -> &'a Vec<Table> {
        self.transaction_tables
            .as_ref()
            .map(|t| &t.current_tables)
            .unwrap_or(self.commited_tables)
    }

    fn tables_mut(&'b mut self) -> &'b mut Vec<Table> {
        self.transaction_tables
            .as_mut()
            .map(|t| &mut t.current_tables)
            .unwrap_or(self.commited_tables)
    }

    pub fn create_snapshot(&mut self) {
        *self.transaction_tables = Some(TransactionTables::new(self.commited_tables.clone()));
    }

    pub fn apply_snapshot(&mut self) {
        // TODO: as we do not have concurrent tranasactions yet in the simulator
        // there is no conflict we are ignoring conflict problems right now
        if let Some(transation_tables) = self.transaction_tables.take() {
            let mut shadow_table = ShadowTablesMut {
                commited_tables: self.commited_tables,
                transaction_tables: &mut None,
            };

            for query in transation_tables.pending_changes {
                // TODO: maybe panic on shadow error here
                let _ = query.shadow(&mut shadow_table);
            }
        }
    }

    pub fn delete_snapshot(&mut self) {
        *self.transaction_tables = None;
    }

    /// Append non transaction queries to the shadow tables
    pub fn add_query(&mut self, query: &Query) {
        assert!(!query.is_transaction());
        if let Some(transaction_tables) = self.transaction_tables {
            transaction_tables.pending_changes.push(query.clone());
        }
    }
}

impl<'a> Deref for ShadowTablesMut<'a> {
    type Target = Vec<Table>;

    fn deref(&self) -> &Self::Target {
        self.tables()
    }
}

impl<'a> DerefMut for ShadowTablesMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.tables_mut()
    }
}

pub(crate) struct SimulatorEnv {
    pub(crate) opts: SimulatorOpts,
    pub profile: Profile,
    pub(crate) connections: Vec<SimConnection>,
    pub(crate) io: Arc<dyn SimIO>,
    pub(crate) db: Option<Arc<Database>>,
    pub(crate) rng: ChaCha8Rng,

    seed: u64,
    pub(crate) paths: Paths,
    pub(crate) type_: SimulationType,
    pub(crate) phase: SimulationPhase,
    pub memory_io: bool,

    /// If connection state is None, means we are not in a transaction
    pub connection_tables: Vec<Option<TransactionTables>>,
    /// Bit map indicating whether a connection has executed a query that is not transaction related
    ///
    /// E.g Select, Insert, Create
    /// and not Begin, Commit, Rollback \
    /// Has max size of 64 to accomodate 64 connections
    connection_last_query: Bitmap<64>,
    // Table data that is committed into the database or wal
    pub committed_tables: Vec<Table>,
}

impl UnwindSafe for SimulatorEnv {}

impl SimulatorEnv {
    pub(crate) fn clone_without_connections(&self) -> Self {
        SimulatorEnv {
            opts: self.opts.clone(),
            io: self.io.clone(),
            db: self.db.clone(),
            rng: self.rng.clone(),
            seed: self.seed,
            paths: self.paths.clone(),
            type_: self.type_,
            phase: self.phase,
            memory_io: self.memory_io,
            profile: self.profile.clone(),
            connections: (0..self.connections.len())
                .map(|_| SimConnection::Disconnected)
                .collect(),
            // TODO: not sure if connection_tables should be recreated instead
            connection_tables: self.connection_tables.clone(),
            connection_last_query: self.connection_last_query,
            committed_tables: self.committed_tables.clone(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.clear_tables();
        self.connections.iter_mut().for_each(|c| c.disconnect());
        self.rng = ChaCha8Rng::seed_from_u64(self.opts.seed);

        let latency_prof = &self.profile.io.latency;

        let io: Arc<dyn SimIO> = if self.memory_io {
            Arc::new(MemorySimIO::new(
                self.opts.seed,
                self.opts.page_size,
                latency_prof.latency_probability,
                latency_prof.min_tick,
                latency_prof.max_tick,
            ))
        } else {
            Arc::new(
                SimulatorIO::new(
                    self.opts.seed,
                    self.opts.page_size,
                    latency_prof.latency_probability,
                    latency_prof.min_tick,
                    latency_prof.max_tick,
                )
                .unwrap(),
            )
        };

        // Remove existing database file
        let db_path = self.get_db_path();
        if db_path.exists() {
            std::fs::remove_file(&db_path).unwrap();
        }

        let wal_path = db_path.with_extension("db-wal");
        if wal_path.exists() {
            std::fs::remove_file(&wal_path).unwrap();
        }
        self.db = None;

        if self.profile.seed_sqlite_autovacuum {
            self.seed_sqlite_autovacuum(&db_path);
        }

        let db = match Database::open_file(
            io.clone(),
            db_path.to_str().unwrap(),
            self.profile.experimental_mvcc,
            self.profile.query.gen_opts.indexes,
        ) {
            Ok(db) => db,
            Err(e) => {
                tracing::error!(%e);
                panic!("error opening simulator test file {db_path:?}: {e:?}");
            }
        };
        self.io = io;
        self.db = Some(db);
    }

    fn seed_sqlite_autovacuum(&self, db_path: &Path) {
        let conn = rusqlite::Connection::open(db_path).unwrap_or_else(|e| {
            panic!(
                "failed to open sqlite connection while seeding auto-vacuum database at {}: {e}",
                db_path.display()
            )
        });
        conn.execute_batch(
            "PRAGMA auto_vacuum=FULL;\
             VACUUM;\
             CREATE TABLE IF NOT EXISTS __sim_autovacuum_seed__(id INTEGER PRIMARY KEY, value TEXT);\
             INSERT OR IGNORE INTO __sim_autovacuum_seed__ VALUES (1,'seed');",
        )
        .unwrap_or_else(|e| {
            panic!(
                "failed to seed sqlite auto-vacuum database at {}: {e}",
                db_path.display()
            )
        });
        drop(conn);
    }

    pub(crate) fn get_db_path(&self) -> PathBuf {
        self.paths.db(&self.type_, &self.phase)
    }

    pub(crate) fn get_plan_path(&self) -> PathBuf {
        self.paths.plan(&self.type_, &self.phase)
    }

    pub(crate) fn clone_as(&self, simulation_type: SimulationType) -> Self {
        let mut env = self.clone_without_connections();
        env.type_ = simulation_type;
        env.clear();
        env
    }

    pub(crate) fn clone_at_phase(&self, phase: SimulationPhase) -> Self {
        let mut env = self.clone_without_connections();
        env.phase = phase;
        env.clear();
        env
    }

    pub fn choose_conn(&self, rng: &mut impl Rng) -> usize {
        rng.random_range(0..self.connections.len())
    }

    /// Rng only used for generating interactions. By having a separate Rng we can guarantee that a particular seed
    /// will always create the same interactions plan, regardless of the changes that happen in the Database code
    pub fn gen_rng(&self) -> ChaCha8Rng {
        // Seed + 1 so that there is no relation with the original seed, and so we have no way to accidently generate
        // the first Create statement twice in a row
        ChaCha8Rng::seed_from_u64(self.seed + 1)
    }
}

impl SimulatorEnv {
    pub(crate) fn new(
        seed: u64,
        cli_opts: &SimulatorCLI,
        paths: Paths,
        simulation_type: SimulationType,
        profile: &Profile,
    ) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let mut opts = SimulatorOpts {
            seed,
            ticks: rng
                .random_range(cli_opts.minimum_tests as usize..=cli_opts.maximum_tests as usize),
            disable_select_optimizer: cli_opts.disable_select_optimizer,
            disable_insert_values_select: cli_opts.disable_insert_values_select,
            disable_double_create_failure: cli_opts.disable_double_create_failure,
            disable_select_limit: cli_opts.disable_select_limit,
            disable_delete_select: cli_opts.disable_delete_select,
            disable_drop_select: cli_opts.disable_drop_select,
            disable_where_true_false_null: cli_opts.disable_where_true_false_null,
            disable_union_all_preserves_cardinality: cli_opts
                .disable_union_all_preserves_cardinality,
            disable_fsync_no_wait: cli_opts.disable_fsync_no_wait,
            disable_faulty_query: cli_opts.disable_faulty_query,
            page_size: 4096, // TODO: randomize this too
            max_interactions: rng.random_range(cli_opts.minimum_tests..=cli_opts.maximum_tests),
            max_time_simulation: cli_opts.maximum_time,
            disable_reopen_database: cli_opts.disable_reopen_database,
            disable_integrity_check: cli_opts.disable_integrity_check,
        };

        // Remove existing database file if it exists
        let db_path = paths.db(&simulation_type, &SimulationPhase::Test);

        if db_path.exists() {
            std::fs::remove_file(&db_path).unwrap();
        }

        let wal_path = db_path.with_extension("db-wal");
        if wal_path.exists() {
            std::fs::remove_file(&wal_path).unwrap();
        }

        let mut profile = profile.clone();
        // Conditionals here so that we can override some profile options from the CLI
        if let Some(mvcc) = cli_opts.experimental_mvcc {
            profile.experimental_mvcc = mvcc;
        }
        if let Some(indexes) = cli_opts.disable_experimental_indexes {
            profile.query.gen_opts.indexes = indexes;
        }
        if let Some(latency_prob) = cli_opts.latency_probability {
            profile.io.latency.latency_probability = latency_prob;
        }
        if let Some(max_tick) = cli_opts.max_tick {
            profile.io.latency.max_tick = max_tick;
        }
        if let Some(min_tick) = cli_opts.min_tick {
            profile.io.latency.min_tick = min_tick;
        }
        if cli_opts.differential {
            // Disable faults when running against sqlite as we cannot control faults on it
            profile.io.enable = false;
            // Disable limits due to differences in return order from turso and rusqlite
            opts.disable_select_limit = true;

            // There is no `ALTER COLUMN` in SQLite
            profile.query.gen_opts.query.alter_table.alter_column = false;
        }

        profile.validate().unwrap();

        let latency_prof = &profile.io.latency;

        let io: Arc<dyn SimIO> = if cli_opts.memory_io {
            Arc::new(MemorySimIO::new(
                seed,
                opts.page_size,
                latency_prof.latency_probability,
                latency_prof.min_tick,
                latency_prof.max_tick,
            ))
        } else {
            Arc::new(
                SimulatorIO::new(
                    seed,
                    opts.page_size,
                    latency_prof.latency_probability,
                    latency_prof.min_tick,
                    latency_prof.max_tick,
                )
                .unwrap(),
            )
        };

        let db = match Database::open_file(
            io.clone(),
            db_path.to_str().unwrap(),
            profile.experimental_mvcc,
            profile.query.gen_opts.indexes,
        ) {
            Ok(db) => db,
            Err(e) => {
                panic!("error opening simulator test file {db_path:?}: {e:?}");
            }
        };

        let connections = (0..profile.max_connections)
            .map(|_| SimConnection::Disconnected)
            .collect::<Vec<_>>();

        SimulatorEnv {
            opts,
            connections,
            paths,
            rng,
            seed,
            io,
            db: Some(db),
            type_: simulation_type,
            phase: SimulationPhase::Test,
            memory_io: cli_opts.memory_io,
            profile: profile.clone(),
            committed_tables: Vec::new(),
            connection_tables: vec![None; profile.max_connections],
            connection_last_query: Bitmap::new(),
        }
    }

    pub(crate) fn connect(&mut self, connection_index: usize) {
        if connection_index >= self.connections.len() {
            panic!("connection index out of bounds");
        }

        if self.connections[connection_index].is_connected() {
            trace!("Connection {connection_index} is already connected, skipping reconnection");
            return;
        }

        match self.type_ {
            SimulationType::Default | SimulationType::Doublecheck => {
                self.connections[connection_index] = SimConnection::LimboConnection(
                    self.db
                        .as_ref()
                        .expect("db to be Some")
                        .connect()
                        .expect("Failed to connect to Limbo database"),
                );
            }
            SimulationType::Differential => {
                self.connections[connection_index] = SimConnection::SQLiteConnection(
                    rusqlite::Connection::open(self.get_db_path())
                        .expect("Failed to open SQLite connection"),
                );
            }
        };
    }

    /// Clears the commited tables and the connection tables
    pub fn clear_tables(&mut self) {
        self.committed_tables.clear();
        self.connection_tables.iter_mut().for_each(|t| *t = None);
        self.connection_last_query = Bitmap::new();
    }

    // TODO: does not yet create the appropriate context to avoid WriteWriteConflitcs
    pub fn connection_context(&self, conn_index: usize) -> impl GenerationContext {
        struct ConnectionGenContext<'a> {
            tables: &'a Vec<sql_generation::model::table::Table>,
            opts: &'a sql_generation::generation::Opts,
        }

        impl<'a> GenerationContext for ConnectionGenContext<'a> {
            fn tables(&self) -> &Vec<sql_generation::model::table::Table> {
                self.tables
            }

            fn opts(&self) -> &sql_generation::generation::Opts {
                self.opts
            }
        }

        let tables = self.get_conn_tables(conn_index).tables();

        ConnectionGenContext {
            opts: &self.profile.query.gen_opts,
            tables,
        }
    }

    pub fn conn_in_transaction(&self, conn_index: usize) -> bool {
        self.connection_tables
            .get(conn_index)
            .is_some_and(|t| t.is_some())
    }

    pub fn has_conn_executed_query_after_transaction(&self, conn_index: usize) -> bool {
        self.connection_last_query.get(conn_index)
    }

    pub fn update_conn_last_interaction(&mut self, conn_index: usize, query: Option<&Query>) {
        // If the conn will execute a transaction statement then we set the bitmap to false
        // to indicate we have not executed any queries yet after the transaction begun
        let value = query.is_some_and(|query| {
            matches!(
                query,
                Query::Begin(..) | Query::Commit(..) | Query::Rollback(..)
            )
        });
        self.connection_last_query.set(conn_index, value);
    }

    pub fn rollback_conn(&mut self, conn_index: usize) {
        Rollback.shadow(&mut self.get_conn_tables_mut(conn_index));
        self.update_conn_last_interaction(conn_index, Some(&Query::Rollback(Rollback)));
    }

    pub fn get_conn_tables(&self, conn_index: usize) -> ShadowTables<'_> {
        ShadowTables {
            transaction_tables: self.connection_tables.get(conn_index).unwrap().as_ref(),
            commited_tables: &self.committed_tables,
        }
    }

    pub fn get_conn_tables_mut(&mut self, conn_index: usize) -> ShadowTablesMut<'_> {
        ShadowTablesMut {
            transaction_tables: self.connection_tables.get_mut(conn_index).unwrap(),
            commited_tables: &mut self.committed_tables,
        }
    }
}

pub(crate) enum SimConnection {
    LimboConnection(Arc<turso_core::Connection>),
    SQLiteConnection(rusqlite::Connection),
    Disconnected,
}

impl SimConnection {
    pub(crate) fn is_connected(&self) -> bool {
        match self {
            SimConnection::LimboConnection(_) | SimConnection::SQLiteConnection(_) => true,
            SimConnection::Disconnected => false,
        }
    }
    pub(crate) fn disconnect(&mut self) {
        let conn = mem::replace(self, SimConnection::Disconnected);

        match conn {
            SimConnection::LimboConnection(conn) => {
                conn.close().unwrap();
            }
            SimConnection::SQLiteConnection(conn) => {
                conn.close().unwrap();
            }
            SimConnection::Disconnected => {}
        }
    }
}

impl Display for SimConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SimConnection::LimboConnection(_) => {
                write!(f, "LimboConnection")
            }
            SimConnection::SQLiteConnection(_) => {
                write!(f, "SQLiteConnection")
            }
            SimConnection::Disconnected => {
                write!(f, "Disconnected")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SimulatorOpts {
    pub(crate) seed: u64,
    pub(crate) ticks: usize,

    pub(crate) disable_select_optimizer: bool,
    pub(crate) disable_insert_values_select: bool,
    pub(crate) disable_double_create_failure: bool,
    pub(crate) disable_select_limit: bool,
    pub(crate) disable_delete_select: bool,
    pub(crate) disable_drop_select: bool,
    pub(crate) disable_where_true_false_null: bool,
    pub(crate) disable_union_all_preserves_cardinality: bool,
    pub(crate) disable_fsync_no_wait: bool,
    pub(crate) disable_faulty_query: bool,
    pub(crate) disable_reopen_database: bool,
    pub(crate) disable_integrity_check: bool,

    pub(crate) max_interactions: u32,
    pub(crate) page_size: usize,
    pub(crate) max_time_simulation: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Paths {
    pub(crate) base: PathBuf,
    pub(crate) history: PathBuf,
}

impl Paths {
    pub(crate) fn new(output_dir: &Path) -> Self {
        Paths {
            base: output_dir.to_path_buf(),
            history: PathBuf::from(output_dir).join("history.txt"),
        }
    }

    fn path_(&self, type_: &SimulationType, phase: &SimulationPhase) -> PathBuf {
        match (type_, phase) {
            (SimulationType::Default, SimulationPhase::Test) => self.base.join(Path::new("test")),
            (SimulationType::Default, SimulationPhase::Shrink) => {
                self.base.join(Path::new("shrink"))
            }
            (SimulationType::Differential, SimulationPhase::Test) => {
                self.base.join(Path::new("diff"))
            }
            (SimulationType::Differential, SimulationPhase::Shrink) => {
                self.base.join(Path::new("diff_shrink"))
            }
            (SimulationType::Doublecheck, SimulationPhase::Test) => {
                self.base.join(Path::new("doublecheck"))
            }
            (SimulationType::Doublecheck, SimulationPhase::Shrink) => {
                self.base.join(Path::new("doublecheck_shrink"))
            }
        }
    }

    pub(crate) fn db(&self, type_: &SimulationType, phase: &SimulationPhase) -> PathBuf {
        self.path_(type_, phase).with_extension("db")
    }
    pub(crate) fn plan(&self, type_: &SimulationType, phase: &SimulationPhase) -> PathBuf {
        self.path_(type_, phase).with_extension("sql")
    }

    pub fn delete_all_files(&self) {
        if self.base.exists() {
            let res = std::fs::remove_dir_all(&self.base);
            if res.is_err() {
                tracing::error!(error = %res.unwrap_err(),"failed to remove directory");
            }
        }
    }
}
