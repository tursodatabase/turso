use std::{fmt::Display, mem, sync::Arc};

use turso_core::{Database, IO};

use crate::model::table::{SimValue, Table};

pub mod query;
pub mod table;

pub trait SimulatorEnv {
    /// List all tables
    fn tables(&self) -> &[Table];
    /// List all tables with a mutable reference
    fn tables_mut(&mut self) -> &mut [Table];
    /// Add a table
    fn add_table(&mut self, table: Table);
    /// Remove a table by name
    fn remove_table(&mut self, table_name: &str);
    /// Gets the simulator options
    fn opts(&self) -> &SimulatorOpts;
    fn connections(&self) -> &[SimConnection];
    fn connections_mut(&mut self) -> &mut Vec<SimConnection>;
    fn db_path(&self) -> &str;
    fn io(&self) -> Arc<dyn IO>;
    fn get_db(&self) -> Arc<Database>;
    fn set_db(&mut self, db: Arc<Database>);
}

pub trait ConnectionTrait
where
    Self: std::marker::Sized + Clone,
{
    fn is_connected(&self) -> bool;
    fn disconnect(&mut self);
}

pub enum SimConnection {
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
pub struct SimulatorOpts {
    pub ticks: usize,
    pub max_connections: usize,
    pub max_tables: usize,
    // this next options are the distribution of workload where read_percent + write_percent +
    // delete_percent == 100%
    pub create_percent: f64,
    pub create_index_percent: f64,
    pub read_percent: f64,
    pub write_percent: f64,
    pub delete_percent: f64,
    pub update_percent: f64,
    pub drop_percent: f64,

    pub disable_select_optimizer: bool,
    pub disable_insert_values_select: bool,
    pub disable_double_create_failure: bool,
    pub disable_select_limit: bool,
    pub disable_delete_select: bool,
    pub disable_drop_select: bool,
    pub disable_fsync_no_wait: bool,
    pub disable_faulty_query: bool,
    pub disable_reopen_database: bool,

    pub max_interactions: usize,
    pub page_size: usize,
    pub max_time_simulation: usize,
}

pub trait Shadow {
    fn shadow<E: SimulatorEnv>(&self, env: &mut E) -> Vec<Vec<SimValue>>;
}
