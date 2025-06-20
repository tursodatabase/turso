use std::path::Path;
use std::sync::Arc;

use limbo_sim::model::{table::Table, SimConnection, SimulatorEnv, SimulatorOpts};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::{Database, IO};

use crate::runner::io::SimulatorIO;

use super::cli::SimulatorCLI;

pub(crate) struct LimboSimulatorEnv {
    pub(crate) opts: SimulatorOpts,
    pub(crate) tables: Vec<Table>,
    pub(crate) connections: Vec<SimConnection>,
    pub(crate) io: Arc<SimulatorIO>,
    pub(crate) db: Arc<Database>,
    pub(crate) rng: ChaCha8Rng,
    pub(crate) db_path: String,
}

impl SimulatorEnv for LimboSimulatorEnv {
    fn tables(&self) -> &[Table] {
        &self.tables
    }

    fn tables_mut(&mut self) -> &mut [Table] {
        &mut self.tables
    }

    fn add_table(&mut self, table: Table) {
        self.tables.push(table);
    }

    fn remove_table(&mut self, table_name: &str) {
        self.tables.retain(|t| t.name != table_name);
    }

    fn opts(&self) -> &SimulatorOpts {
        &self.opts
    }

    fn connections(&self) -> &[SimConnection] {
        &self.connections
    }

    fn connections_mut(&mut self) -> &mut Vec<SimConnection> {
        &mut self.connections
    }

    fn db_path(&self) -> &str {
        &self.db_path
    }

    fn io(&self) -> Arc<dyn IO> {
        self.io.clone()
    }

    fn get_db(&self) -> Arc<Database> {
        self.db.clone()
    }

    fn set_db(&mut self, db: Arc<Database>) {
        self.db = db;
    }
}

impl LimboSimulatorEnv {
    pub(crate) fn new(seed: u64, cli_opts: &SimulatorCLI, db_path: &Path) -> Self {
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
            panic!(
                "Summed total {} is not equal to total {}",
                summed_total, total
            );
        }

        let opts = SimulatorOpts {
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
            disable_fsync_no_wait: cli_opts.disable_fsync_no_wait,
            disable_faulty_query: cli_opts.disable_faulty_query,
            page_size: 4096, // TODO: randomize this too
            max_interactions: rng.gen_range(cli_opts.minimum_tests..=cli_opts.maximum_tests),
            max_time_simulation: cli_opts.maximum_time,
            disable_reopen_database: cli_opts.disable_reopen_database,
        };

        let io =
            Arc::new(SimulatorIO::new(seed, opts.page_size, cli_opts.latency_probability).unwrap());

        // Remove existing database file if it exists
        if db_path.exists() {
            std::fs::remove_file(db_path).unwrap();
        }

        let wal_path = db_path.with_extension("db-wal");
        if wal_path.exists() {
            std::fs::remove_file(wal_path).unwrap();
        }

        let db = match Database::open_file(io.clone(), db_path.to_str().unwrap(), false, false) {
            Ok(db) => db,
            Err(e) => {
                panic!("error opening simulator test file {:?}: {:?}", db_path, e);
            }
        };

        let connections = (0..opts.max_connections)
            .map(|_| SimConnection::Disconnected)
            .collect::<Vec<_>>();

        LimboSimulatorEnv {
            opts,
            tables: Vec::new(),
            connections,
            rng,
            io,
            db,
            db_path: db_path.to_str().unwrap().to_string(),
        }
    }

    pub(crate) fn clone_without_connections(&self) -> Self {
        LimboSimulatorEnv {
            opts: self.opts.clone(),
            tables: self.tables.clone(),
            connections: (0..self.connections.len())
                .map(|_| SimConnection::Disconnected)
                .collect(),
            io: self.io.clone(),
            db: self.db.clone(),
            db_path: self.db_path.clone(),
            rng: self.rng.clone(),
        }
    }
}
