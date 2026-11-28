mod abandoned_create_index;
mod abandoned_statement_pager;
mod alloc;
mod alter_table;
mod assert_details;
#[macro_use]
mod assertions;
mod attach;
mod blob_io;
mod checkpoint_crash_atomicity;
mod checkpoint_stale_page_size;
mod checkpoint_write_failure;
mod common;
mod conflict_resolution;
mod connection;
mod custom_types;
mod database;
mod database_leak;
mod database_open;
mod dialect;
mod expr_depth_stack_overflow;
mod external_apis;
mod functions;
mod fuzz_transaction;
mod incremental_view;
mod index_method;
mod integrity_check;
mod lossy_index_keys;
#[cfg(feature = "io_memory_yield")]
mod memory_yield;
#[cfg(all(
    feature = "fs",
    any(unix, target_os = "windows"),
    target_pointer_width = "64",
    any(not(target_os = "windows"), feature = "win_iocp")
))]
mod multiprocess;
mod mvcc;
mod native_extension;
mod page1_allocation_race;
mod pragma;
mod query_processing;
mod query_timeout;
mod queued_io;
mod reindex;
mod schema;
mod security;
mod statement;
mod statement_lifecycle;
mod statement_metadata;
mod statement_reset;
mod stats;
mod stmt_journal;
mod stmt_readonly;
mod storage;
mod suspended_statement_checkpoint;
mod table_overlay;
#[cfg(all(feature = "fs", not(target_family = "wasm")))]
mod temp_file;
mod transaction_errors;
mod trigger;
mod unreliable_io;
mod views;
mod wal;

#[cfg(test)]
mod tests {
    use tracing_subscriber::EnvFilter;

    #[ctor::ctor]
    fn init() {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_ansi(false)
            .init();
    }
}
