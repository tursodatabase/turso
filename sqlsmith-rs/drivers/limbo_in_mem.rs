use super::DatabaseDriver;
use anyhow::Result;
use turso::{Builder, Connection, Database};
use log::info;
use once_cell::sync::OnceCell;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct LimboDriver {
    conn: Connection,
}

// Global static database instance
static GLOBAL_DB: OnceCell<Arc<Database>> = OnceCell::new();
static INIT_DONE: OnceCell<AtomicBool> = OnceCell::new();

impl LimboDriver {
    pub async fn new() -> Result<Self> {
        let db = GLOBAL_DB.get_or_try_init(|| {
            // Run the blocking DB creation in a separate thread to avoid nested runtime
            std::thread::spawn(|| {
                let rt = tokio::runtime::Runtime::new()?;
                let db = rt.block_on(async { Builder::new_local(":memory:").build().await })?;
                Ok::<Arc<Database>, anyhow::Error>(Arc::new(db))
            })
            .join()
            .map_err(|e| anyhow::anyhow!("Thread panicked: {:?}", e))?
        })?.clone();

        let conn = db.connect()?;
        let driver = Self { conn };

        // Only initialize schema once globally
        let init_flag = INIT_DONE.get_or_init(|| AtomicBool::new(false));
        if !init_flag.swap(true, Ordering::SeqCst) {
            info!("Initializing Limbo database...");
            driver.init().await?;
        }

        Ok(driver)
    }

    async fn init(&self) -> Result<()> {
        info!("(Limbo) Executing init SQL from assets/limbo/tpcc-create-table.sql...");
        let sql_file_path = Path::new("assets/limbo/tpcc-create-table.sql");
        let sql_content = fs::read_to_string(sql_file_path)
            .map_err(|e| anyhow::anyhow!("Failed to read SQL file: {:?}: {}", sql_file_path, e))?;

        // Split and execute each statement
        for stmt in sql_content.split(';') {
            let stmt = stmt.trim();
            if !stmt.is_empty() {
                self.conn
                    .execute(stmt, ())
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to execute Limbo init SQL: {}\nError: {}", stmt, e))?;
            }
        }
        info!("(Limbo) TPC-C tables created successfully.");
        Ok(())
    }

    pub fn get_connection(&self) -> &Connection {
        &self.conn
    }
}

impl DatabaseDriver for LimboDriver {
    fn exec(&self, sql: &str) -> Result<usize> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let result = self.conn.execute(sql, ()).await?;
            Ok(result as usize)
        })
    }

    fn query(&self, sql: &str) -> Result<usize> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let mut rows = self.conn.query(sql, ()).await?;
            let mut count = 0;
            while let Ok(Some(_)) = rows.next().await {
                count += 1;
            }
            Ok(count)
        })
    }

    fn get_connection(&self) -> &dyn std::any::Any {
        &self.conn
    }
}
