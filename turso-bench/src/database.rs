use crate::{config::BenchConfig, metrics::OperationMetrics, Result};
use rand::Rng;
use std::sync::Arc;
use turso_core::{Database, Connection, PlatformIO, OpenFlags, MemoryIO, DatabaseStorage};

/// Database operation modes
#[derive(Debug, Clone, Copy)]
pub enum OperationMode {
    Insert = 0,
    Update = 1,
    Delete = 2,
}

impl TryFrom<u8> for OperationMode {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(OperationMode::Insert),
            1 => Ok(OperationMode::Update),
            2 => Ok(OperationMode::Delete),
            _ => anyhow::bail!("Invalid operation mode: {}", value),
        }
    }
}

/// Manages database connections and operations for benchmarking
pub struct DatabaseManager {
    databases: Vec<Arc<Database>>,
    connections: Vec<Arc<Connection>>,
    config: BenchConfig,
    operation_mode: OperationMode,
}

impl DatabaseManager {
    /// Create a new database manager
    pub async fn new(config: BenchConfig, thread_id: u32) -> Result<Self> {
        let operation_mode = OperationMode::try_from(config.access_mode)?;
        let mut databases = Vec::new();
        let mut connections = Vec::new();

        // Create the directory if it doesn't exist and not using memory
        if config.path != ":memory:" {
            std::fs::create_dir_all(&config.path)?;
        }

        // Create databases and connections
        for db_index in 0..config.num_databases {
            let db_path = config.get_db_path(thread_id, db_index);
            
            let io = Arc::new(PlatformIO::new());
            let db = if db_path.starts_with(":memory:") {
                // For memory databases, create a MemoryIO-based storage
                let memory_storage: Arc<dyn DatabaseStorage> = Arc::new(MemoryIO::new());
                Database::open_with_flags(
                    io,
                    &db_path,
                    memory_storage,
                    OpenFlags::default(),
                    config.enable_mvcc,
                    config.enable_indexes,
                    config.enable_views,
                )?
            } else {
                Database::open_file_with_flags(
                    io,
                    &db_path,
                    OpenFlags::default(),
                    config.enable_mvcc,
                    config.enable_indexes,
                    config.enable_views,
                )?
            };

            let conn = db.connect()?;
            
            databases.push(db);
            connections.push(conn);
        }

        let mut manager = Self {
            databases,
            connections,
            config: config.clone(),
            operation_mode,
        };

        // Initialize database schema
        manager.initialize_schema().await?;

        Ok(manager)
    }

    /// Initialize the database schema
    async fn initialize_schema(&mut self) -> Result<()> {
        for (db_index, conn) in self.connections.iter().enumerate() {
            // Set page size
            conn.execute("PRAGMA page_size = 4096;")?;
            
            // Create tables
            for table_index in 0..self.config.num_tables {
                let table_name = self.config.get_table_name(table_index);
                let create_sql = format!(
                    "CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY, Value TEXT NOT NULL, creation_date INTEGER);",
                    table_name
                );
                conn.execute(&create_sql)?;
            }

            // For update and delete operations, populate with initial data
            if matches!(self.operation_mode, OperationMode::Update | OperationMode::Delete) {
                self.populate_initial_data(db_index).await?;
            }
        }

        Ok(())
    }

    /// Populate initial data for update/delete operations
    async fn populate_initial_data(&self, db_index: usize) -> Result<()> {
        let conn = &self.connections[db_index];
        let insert_str = "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjj";

        conn.execute("BEGIN TRANSACTION;")?;

        for table_index in 0..self.config.num_tables {
            let table_name = self.config.get_table_name(table_index);
            
            for i in 0..self.config.transactions {
                let insert_sql = format!(
                    "INSERT INTO {} (id, Value) VALUES ({}, '{}');",
                    table_name, i, insert_str
                );
                conn.execute(&insert_sql)?;
            }
        }

        conn.execute("COMMIT;")?;
        Ok(())
    }

    /// Execute benchmark operations
    pub async fn execute_operations(&self, metrics: &mut OperationMetrics) -> Result<()> {
        let mut rng = rand::thread_rng();

        for tx_index in 0..self.config.transactions {
            let start_time = std::time::Instant::now();

            for (db_index, conn) in self.connections.iter().enumerate() {
                conn.execute("BEGIN TRANSACTION;")?;

                for table_index in 0..self.config.num_tables {
                    let table_name = self.config.get_table_name(table_index);
                    
                    match self.operation_mode {
                        OperationMode::Insert => {
                            let id = if self.config.random_insert {
                                rng.gen_range(0..self.config.transactions * 10)
                            } else {
                                tx_index
                            };
                            
                            let insert_str = "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjj";
                            let sql = format!(
                                "INSERT INTO {} (id, Value) VALUES ({}, '{}');",
                                table_name, id, insert_str
                            );
                            conn.execute(&sql)?;
                        }
                        OperationMode::Update => {
                            let id = rng.gen_range(1..=self.config.transactions);
                            let update_str = "ffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee";
                            let sql = format!(
                                "UPDATE {} SET Value = '{}' WHERE id = {};",
                                table_name, update_str, id
                            );
                            conn.execute(&sql)?;
                        }
                        OperationMode::Delete => {
                            let id = rng.gen_range(1..=self.config.transactions);
                            let sql = format!(
                                "DELETE FROM {} WHERE id = {};",
                                table_name, id
                            );
                            conn.execute(&sql)?;
                        }
                    }
                }

                conn.execute("COMMIT;")?;
            }

            let duration = start_time.elapsed();
            metrics.record_operation(duration);

            // Add interval if specified
            if self.config.interval_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(self.config.interval_ms)).await;
            }

            // Progress reporting
            if !self.config.quiet {
                let progress = ((tx_index + 1) * 100) / self.config.transactions;
                print!("\r{}%", progress);
                if progress == 100 {
                    println!();
                }
            }
        }

        Ok(())
    }

    /// Get database information for reporting
    pub fn get_database_info(&self) -> Vec<String> {
        let mut info = Vec::new();
        
        for (i, _db) in self.databases.iter().enumerate() {
            info.push(format!("Database {}: {}", i, self.config.get_db_path(0, i as u32)));
        }
        
        info
    }

    /// Close all connections and databases
    pub fn close(&mut self) -> Result<()> {
        // Connections will be dropped automatically
        self.connections.clear();
        self.databases.clear();
        Ok(())
    }
}