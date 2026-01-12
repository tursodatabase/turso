use anyhow::Result;
use serde::{Deserialize, Serialize};

pub mod limbo_in_mem;
pub mod sqlite_in_mem; // <-- 添加这一行

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DRIVER_KIND {
    SQLITE_IN_MEM,
    LIMBO_IN_MEM, // 新增 LIMBO 类型
}

pub trait DatabaseDriver {
    fn exec(&self, sql: &str) -> Result<usize>;
    fn query(&self, sql: &str) -> Result<usize>;
    fn get_connection(&self) -> &dyn std::any::Any;
}

/// Enum to wrap both driver types
pub enum AnyDatabaseDriver {
    Sqlite(sqlite_in_mem::SqliteDriver),
    Limbo(limbo_in_mem::LimboDriver),
}

impl DatabaseDriver for AnyDatabaseDriver {
    fn exec(&self, sql: &str) -> Result<usize> {
        match self {
            AnyDatabaseDriver::Sqlite(driver) => driver.exec(sql),
            AnyDatabaseDriver::Limbo(driver) => driver.exec(sql),
        }
    }
    fn query(&self, sql: &str) -> Result<usize> {
        match self {
            AnyDatabaseDriver::Sqlite(driver) => driver.query(sql),
            AnyDatabaseDriver::Limbo(driver) => driver.query(sql),
        }
    }
    fn get_connection(&self) -> &dyn std::any::Any {
        match self {
            AnyDatabaseDriver::Sqlite(driver) => driver.get_connection(),
            AnyDatabaseDriver::Limbo(driver) => driver.get_connection(),
        }
    }
}

/// 通用接口：根据 DRIVER_KIND 创建驱动和连接
pub async fn new_conn(
    kind: DRIVER_KIND,
) -> Result<AnyDatabaseDriver> {
    match kind {
        DRIVER_KIND::SQLITE_IN_MEM => {
            let driver = sqlite_in_mem::SqliteDriver::new()?;
            Ok(AnyDatabaseDriver::Sqlite(driver))
        }
        DRIVER_KIND::LIMBO_IN_MEM => {
            let driver = limbo_in_mem::LimboDriver::new().await?;
            Ok(AnyDatabaseDriver::Limbo(driver))
        }
    }
}
