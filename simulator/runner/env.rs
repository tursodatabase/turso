use std::fmt::Display;
use std::mem;
use std::ops::Deref;
use std::panic::UnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::Database;

use crate::model::table::Table;

use crate::runner::io::SimulatorIO;

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
pub(crate) struct SimulatorTables {
    pub(crate) tables: Vec<Table>,
    pub(crate) snapshot: Option<Vec<Table>>,
}
impl SimulatorTables {
    pub(crate) fn new() -> Self {
        Self {
            tables: Vec::new(),
            snapshot: None,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.tables.clear();
        self.snapshot = None;
    }

    pub(crate) fn push(&mut self, table: Table) {
        self.tables.push(table);
    }
}
impl Deref for SimulatorTables {
    type Target = Vec<Table>;

    fn deref(&self) -> &Self::Target {
        &self.tables
    }
}

pub(crate) struct SimulatorEnv {
    pub(crate) opts: SimulatorOpts,
    pub(crate) connections: Vec<SimConnection>,
    pub(crate) io: Arc<SimulatorIO>,
    pub(crate) db: Arc<Database>,
    pub(crate) rng: ChaCha8Rng,
    pub(crate) paths: Paths,
    pub(crate) type_: SimulationType,
    pub(crate) phase: SimulationPhase,
    pub(crate) tables: SimulatorTables,
}

impl UnwindSafe for SimulatorEnv {}

impl SimulatorEnv {
    pub(crate) fn clone_without_connections(&self) -> Self {
        SimulatorEnv {
            opts: self.opts.clone(),
            tables: self.tables.clone(),
            connections: (0..self.connections.len())
                .map(|_| SimConnection::Disconnected)
                .collect(),
            io: self.io.clone(),
            db: self.db.clone(),
            rng: self.rng.clone(),
            paths: self.paths.clone(),
            type_: self.type_,
            phase: self.phase,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.tables.clear();
        self.connections.iter_mut().for_each(|c| c.disconnect());
        self.rng = ChaCha8Rng::seed_from_u64(self.opts.seed);

        let io = Arc::new(
            SimulatorIO::new(
                self.opts.seed,
                self.opts.page_size,
                self.opts.latency_probability,
                self.opts.min_tick,
                self.opts.max_tick,
            )
            .unwrap(),
        );

        // Remove existing database file
        let db_path = self.get_db_path();
        if db_path.exists() {
            std::fs::remove_file(&db_path).unwrap();
        }

        let wal_path = db_path.with_extension("db-wal");
        if wal_path.exists() {
            std::fs::remove_file(&wal_path).unwrap();
        }

        let db = match Database::open_file(
            io.clone(),
            db_path.to_str().unwrap(),
            self.opts.experimental_mvcc,
            self.opts.experimental_indexes,
        ) {
            Ok(db) => db,
            Err(e) => {
                tracing::error!(%e);
                panic!("error opening simulator test file {db_path:?}: {e:?}");
            }
        };
        self.io = io;
        self.db = db;
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
}

impl SimulatorEnv {
    pub(crate) fn new(
        seed: u64,
        cli_opts: &SimulatorCLI,
        paths: Paths,
        simulation_type: SimulationType,
    ) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let total = 100.0;

        let mut create_percent = 0.0;
        let mut create_index_percent = 0.0;
        let mut drop_percent = 0.0;
        let mut delete_percent = 0.0;
        let mut update_percent = 0.0;

        let read_percent = rng.gen_range(0.0..=total);
        let write_percent = total - read_percent;

        if !cli_opts.disable_create {
            // Create percent should be 5-15% of the write percent
            create_percent = rng.gen_range(0.05..=0.15) * write_percent;
        }
        if !cli_opts.disable_create_index {
            // Create indexpercent should be 2-5% of the write percent
            create_index_percent = rng.gen_range(0.02..=0.05) * write_percent;
        }
        if !cli_opts.disable_drop {
            // Drop percent should be 2-5% of the write percent
            drop_percent = rng.gen_range(0.02..=0.05) * write_percent;
        }
        if !cli_opts.disable_delete {
            // Delete percent should be 10-20% of the write percent
            delete_percent = rng.gen_range(0.1..=0.2) * write_percent;
        }
        if !cli_opts.disable_update {
            // Update percent should be 10-20% of the write percent
            // TODO: freestyling the percentage
            update_percent = rng.gen_range(0.1..=0.2) * write_percent;
        }

        let write_percent = write_percent
            - create_percent
            - create_index_percent
            - delete_percent
            - drop_percent
            - update_percent;

        let summed_total: f64 = read_percent
            + write_percent
            + create_percent
            + create_index_percent
            + drop_percent
            + update_percent
            + delete_percent;

        let abs_diff = (summed_total - total).abs();
        if abs_diff > 0.0001 {
            panic!("Summed total {summed_total} is not equal to total {total}");
        }

        let opts = SimulatorOpts {
            seed,
            ticks: rng.gen_range(cli_opts.minimum_tests..=cli_opts.maximum_tests),
            max_connections: 1, // TODO: for now let's use one connection as we didn't implement
            // correct transactions processing
            max_tables: rng.gen_range(0..128),
            create_percent,
            create_index_percent,
            read_percent,
            write_percent,
            delete_percent,
            drop_percent,
            update_percent,
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
            enable_faulty_query: cli_opts.enable_faulty_query,
            page_size: 4096, // TODO: randomize this too
            max_interactions: rng.gen_range(cli_opts.minimum_tests..=cli_opts.maximum_tests),
            max_time_simulation: cli_opts.maximum_time,
            disable_reopen_database: cli_opts.disable_reopen_database,
            latency_probability: cli_opts.latency_probability,
            experimental_mvcc: cli_opts.experimental_mvcc,
            experimental_indexes: !cli_opts.disable_experimental_indexes,
            min_tick: cli_opts.min_tick,
            max_tick: cli_opts.max_tick,
        };

        let io = Arc::new(
            SimulatorIO::new(
                seed,
                opts.page_size,
                cli_opts.latency_probability,
                cli_opts.min_tick,
                cli_opts.max_tick,
            )
            .unwrap(),
        );

        // Remove existing database file if it exists
        let db_path = paths.db(&simulation_type, &SimulationPhase::Test);

        if db_path.exists() {
            std::fs::remove_file(&db_path).unwrap();
        }

        let wal_path = db_path.with_extension("db-wal");
        if wal_path.exists() {
            std::fs::remove_file(&wal_path).unwrap();
        }

        let db = match Database::open_file(
            io.clone(),
            db_path.to_str().unwrap(),
            opts.experimental_mvcc,
            opts.experimental_indexes,
        ) {
            Ok(db) => db,
            Err(e) => {
                panic!("error opening simulator test file {db_path:?}: {e:?}");
            }
        };

        let connections = (0..opts.max_connections)
            .map(|_| SimConnection::Disconnected)
            .collect::<Vec<_>>();

        SimulatorEnv {
            opts,
            tables: SimulatorTables::new(),
            connections,
            paths,
            rng,
            io,
            db,
            type_: simulation_type,
            phase: SimulationPhase::Test,
        }
    }

    pub(crate) fn connect(&mut self, connection_index: usize) {
        if connection_index >= self.connections.len() {
            panic!("connection index out of bounds");
        }

        if self.connections[connection_index].is_connected() {
            log::trace!(
                "Connection {connection_index} is already connected, skipping reconnection"
            );
            return;
        }

        match self.type_ {
            SimulationType::Default | SimulationType::Doublecheck => {
                self.connections[connection_index] = SimConnection::LimboConnection(
                    self.db
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
}

pub trait ConnectionTrait
where
    Self: std::marker::Sized + Clone,
{
    fn is_connected(&self) -> bool;
    fn disconnect(&mut self);
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
    pub(crate) max_connections: usize,
    pub(crate) max_tables: usize,
    // this next options are the distribution of workload where read_percent + write_percent +
    // delete_percent == 100%
    pub(crate) create_percent: f64,
    pub(crate) create_index_percent: f64,
    pub(crate) read_percent: f64,
    pub(crate) write_percent: f64,
    pub(crate) delete_percent: f64,
    pub(crate) update_percent: f64,
    pub(crate) drop_percent: f64,

    pub(crate) disable_select_optimizer: bool,
    pub(crate) disable_insert_values_select: bool,
    pub(crate) disable_double_create_failure: bool,
    pub(crate) disable_select_limit: bool,
    pub(crate) disable_delete_select: bool,
    pub(crate) disable_drop_select: bool,
    pub(crate) disable_where_true_false_null: bool,
    pub(crate) disable_union_all_preserves_cardinality: bool,
    pub(crate) disable_fsync_no_wait: bool,
    pub(crate) enable_faulty_query: bool,
    pub(crate) disable_reopen_database: bool,

    pub(crate) max_interactions: usize,
    pub(crate) page_size: usize,
    pub(crate) max_time_simulation: usize,
    pub(crate) latency_probability: usize,
    pub(crate) experimental_mvcc: bool,
    pub(crate) experimental_indexes: bool,
    pub min_tick: u64,
    pub max_tick: u64,
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
