//! The storage layer.
//!
//! This module contains the storage layer for Limbo. The storage layer is
//! responsible for managing access to the database and its pages. The main
//! interface to the storage layer is the `Pager` struct, which is
//! responsible for managing the database file and the pages it contains.
//!
//! Pages in a database are stored in one of the following to data structures:
//! `DatabaseStorage` or `Wal`. The `DatabaseStorage` trait is responsible
//! for reading and writing pages to the database file, either local or
//! remote. The `Wal` struct is responsible for managing the write-ahead log
//! for the database, also either local or remote.
pub(crate) mod btree;
pub(crate) mod buffer_pool;
pub mod database;
pub(crate) mod encryption;
pub(crate) mod page_cache;
pub(crate) mod page_error_check;
#[allow(clippy::arc_with_non_send_sync)]
pub(crate) mod pager;
#[allow(dead_code)]
pub(super) mod slot_bitmap;
pub(crate) mod sqlite3_ondisk;
mod state_machines;
#[allow(clippy::arc_with_non_send_sync)]
pub(crate) mod wal;

#[macro_export]
macro_rules! return_corrupt {
    ($msg:expr) => {
        return Err(LimboError::Corrupt($msg.into()));
    };
}
