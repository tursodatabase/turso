// 仅用于公开 common 子模块
pub mod alter_table_stmt_common;
pub mod create_table_stmt_common;
pub mod create_trigger_stmt_common;
pub mod data_type;
pub mod datefunc_stmt_common; // Added module for date functions
pub mod delete_stmt_common;
pub mod drop_trigger_stmt_common; // New module declaration
pub mod insert_stmt_common;
pub mod pragma_stmt_common;
pub mod select_stmt_common;
pub mod transaction_stmt_common;
pub mod update_stmt_common;
pub mod vacuum_stmt_common;

// 通用 SQL 语句类型定义，供 limbo 和 sqlite 共享
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SqlKind {
    Select,
    Insert,
    Update,
    Delete,
    Vacuum,
    Pragma,
    CreateTrigger,
    DropTrigger,
    DateFunc, // Added DateFunc SqlKind
    AlterTable,
    CreateTable,
    Transaction,
}

use sqlsmith_rs_common::rand_by_seed::LcgRng;

pub enum DriverKind {
    Sqlite,
    Limbo,
}

// Helper function to handle driver-specific connection downcasting and stmt generation
fn call_driver_get_stmt_by_seed(
    driver_kind: DriverKind,
    conn: &dyn std::any::Any,
    rng: &mut LcgRng,
    kind: SqlKind,
) -> Option<String> {
    match driver_kind {
        DriverKind::Sqlite => {
            if let Some(sqlite_conn) = conn.downcast_ref::<rusqlite::Connection>() {
                crate::generators::sqlite::get_stmt_by_seed(sqlite_conn, rng, kind)
            } else {
                None
            }
        }
        DriverKind::Limbo => {
            if let Some(limbo_conn) = conn.downcast_ref::<turso::Connection>() {
                crate::generators::limbo::get_stmt_by_seed(limbo_conn, rng, kind)
            } else {
                None
            }
        }
    }
}

pub fn gen_stmt(
    sql_kind: SqlKind,
    driver_kind: DriverKind,
    conn: &dyn std::any::Any,
    rng: &mut LcgRng,
) -> Option<String> {
    match sql_kind {
        SqlKind::Select => call_driver_get_stmt_by_seed(driver_kind, conn, rng, SqlKind::Select),
        SqlKind::Insert => call_driver_get_stmt_by_seed(driver_kind, conn, rng, SqlKind::Insert),
        SqlKind::Update => call_driver_get_stmt_by_seed(driver_kind, conn, rng, SqlKind::Update),
        SqlKind::Delete => call_driver_get_stmt_by_seed(driver_kind, conn, rng, SqlKind::Delete),
        SqlKind::CreateTrigger => {
            call_driver_get_stmt_by_seed(driver_kind, conn, rng, SqlKind::CreateTrigger)
        }
        SqlKind::DropTrigger => {
            call_driver_get_stmt_by_seed(driver_kind, conn, rng, SqlKind::DropTrigger)
        }
        SqlKind::Vacuum => crate::generators::common::vacuum_stmt_common::gen_vacuum_stmt(),
        SqlKind::Pragma => match driver_kind {
            DriverKind::Sqlite => {
                if let Some(sqlite_conn) = conn.downcast_ref::<rusqlite::Connection>() {
                    crate::generators::common::pragma_stmt_common::get_pragma_stmt_by_seed(rng)
                } else {
                    None
                }
            }
            DriverKind::Limbo => None,
        },
        SqlKind::DateFunc => {
            crate::generators::common::datefunc_stmt_common::gen_datefunc_stmt(rng)
        }
        SqlKind::AlterTable => {
            call_driver_get_stmt_by_seed(driver_kind, conn, rng, SqlKind::AlterTable)
        }
        SqlKind::CreateTable => {
            call_driver_get_stmt_by_seed(driver_kind, conn, rng, SqlKind::CreateTable)
        }
        SqlKind::Transaction => {
            call_driver_get_stmt_by_seed(driver_kind, conn, rng, SqlKind::Transaction)
        }
    }
}
