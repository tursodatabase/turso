// src/drivers/sqlite.rs

use super::DatabaseDriver;
use anyhow::Result;
use log::info;
use rusqlite::Connection;
use std::fs;
use std::path::Path;

/// Represents a SQLite database driver.
pub struct SqliteDriver {
    conn: Connection,
}

impl SqliteDriver {
    /// Creates a new `SqliteDriver` instance for in-memory database.
    pub fn new() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let driver = Self { conn };

        // 初始化数据库
        info!("Initializing SQLite in-memory database...");
        driver.init()?;

        // 验证初始化结果
        if !driver.verify()? {
            anyhow::bail!("SQLite verify failed after init.");
        }

        Ok(driver)
    }

    fn init(&self) -> Result<()> {
        info!("(SQLite) Executing init SQL from assets/sqlite/tpcc-create-table.sql...");
        let sql_file_path = Path::new("assets/sqlite/tpcc-create-table.sql");
        let sql_content = fs::read_to_string(sql_file_path)
            .map_err(|e| anyhow::anyhow!("Failed to read SQL file: {:?}: {}", sql_file_path, e))?;

        self.conn
            .execute_batch(&sql_content)
            .map_err(|e| anyhow::anyhow!("Failed to execute SQLite init SQL batch: {}", e))?;
        info!("(SQLite) TPC-C tables created successfully.");
        Ok(())
    }

    fn verify(&self) -> Result<bool> {
        let count: i32 = self.conn.query_row(
            "SELECT count(*) FROM warehouse",
            rusqlite::params![],
            |row| row.get(0),
        )?;
        if count != 0 {
            return Ok(false);
        }

        let insert_sql = "INSERT INTO warehouse (w_id, w_name, w_ytd, w_tax, w_street_1, w_street_2, w_city, w_state, w_zip) \
                          VALUES (1, 'test', 0, 0, 'a', 'b', 'c', 'd', 'e')";
        self.conn.execute(insert_sql, rusqlite::params![])?;

        let (count, name): (i32, String) = self.conn.query_row(
            "SELECT count(*), w_name FROM warehouse",
            rusqlite::params![],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if count != 1 || name != "test" {
            self.conn
                .execute("DELETE FROM warehouse WHERE w_id=1", rusqlite::params![])?;
            return Ok(false);
        }

        self.conn
            .execute("DELETE FROM warehouse WHERE w_id=1", rusqlite::params![])?;

        let count: i32 = self.conn.query_row(
            "SELECT count(*) FROM warehouse",
            rusqlite::params![],
            |row| row.get(0),
        )?;
        Ok(count == 0)
    }
}

impl DatabaseDriver for SqliteDriver {
    fn exec(&self, sql: &str) -> Result<usize> {
        // 检查是否为查询语句，简单通过常见关键字判断
        let lower_sql = sql.to_lowercase();
        if lower_sql.starts_with("select") || lower_sql.starts_with("pragma") {
            let mut stmt = self.conn.prepare(sql)?;
            let mut rows = stmt.query([])?;
            let mut count = 0;
            while let Some(_) = rows.next()? {
                count += 1;
            }
            Ok(count)
        } else {
            Ok(self.conn.execute(sql, [])?)
        }
    }

    fn query(&self, sql: &str) -> Result<usize> {
        let mut stmt = self.conn.prepare(sql)?;
        let mut rows = stmt.query([])?;
        let mut count = 0;
        while let Some(_) = rows.next()? {
            count += 1;
        }
        Ok(count)
    }

    fn get_connection(&self) -> &dyn std::any::Any {
        &self.conn
    }
}
