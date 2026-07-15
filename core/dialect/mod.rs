//! SQL dialects.
//!
//! The [`Dialect`] trait is the boundary between the engine and the SQL
//! dialect a frontend speaks. The engine owns the mechanics — pages,
//! B-trees, the `sqlite_schema` table itself, bytecode — and consults the
//! dialect wherever the meaning of SQL text is dialect-specific. The
//! [`sqlite`] module owns [`SqliteDialect`], the SQLite implementation,
//! and the catalog tables that ship with every Turso build (`pragma_*`,
//! `json_each`/`json_tree`, `sqlite_dbpage`, `btree_dump`,
//! `sqlite_turso_types`).

pub mod sqlite;

pub use sqlite::SqliteDialect;

/// SQL dialect layered on top of the engine.
///
/// Every [`crate::Database`] carries a dialect, supplied explicitly by
/// every open path and fixed for the lifetime of the database;
/// SQLite-compatible callers pass [`SqliteDialect`]. The engine consults
/// the dialect on every schema load — database open, connection schema
/// reparse, the `ParseSchema` opcode after DDL, and MVCC schema rebuild —
/// so a frontend can store its own schema SQL in `sqlite_schema` and
/// reparse it here.
pub trait Dialect: Send + Sync + 'static {
    /// Stable identifier for this dialect (e.g. "sqlite", "postgres").
    ///
    /// A database file must always be opened with the same dialect it was
    /// created with; the process-wide database registry uses this name to
    /// reject an open whose dialect differs from the already-open instance.
    fn name(&self) -> &'static str;

    /// Parse a `sqlite_schema` `type='table'` row's SQL into a table
    /// definition.
    ///
    /// Rows written by internal engine paths (sequence backing tables,
    /// `sqlite_sequence`) are plain SQLite text and carry no frontend
    /// marker, so every implementation must fall back to SQLite parsing
    /// for text it does not recognize as its own.
    fn parse_table_sql(
        &self,
        sql: &str,
        root_page: i64,
    ) -> crate::Result<crate::schema::BTreeTable>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::BTreeTable;
    use crate::storage::database::DatabaseFile;
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use crate::{Database, DatabaseOpts, MemoryIO, OpenFlags, IO};
    use std::sync::Arc;

    /// A dialect that counts schema-row parses and strips a `/* test */ `
    /// marker before delegating to SQLite parsing, mirroring how a frontend
    /// dialect recognizes its own stored text and falls back to SQLite for
    /// unmarked rows.
    #[derive(Default)]
    struct TestDialect {
        parse_calls: AtomicUsize,
    }

    impl Dialect for TestDialect {
        fn name(&self) -> &'static str {
            "test"
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            self.parse_calls.fetch_add(1, Ordering::SeqCst);
            let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
            BTreeTable::from_sql(sql, root_page)
        }
    }

    fn open_db(
        io: &Arc<dyn IO>,
        path: &str,
        dialect: Arc<dyn Dialect>,
    ) -> crate::Result<Arc<Database>> {
        let file = io.open_file(path, OpenFlags::Create, true)?;
        let db_file = Arc::new(DatabaseFile::new(file));
        Database::open_with_flags_with_allocator(
            io.clone(),
            path,
            db_file,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
            None,
            crate::alloc::DynAllocator::default(),
            dialect,
        )
    }

    #[test]
    fn schema_load_routes_through_dialect() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        {
            let db = open_db(&io, "dialect-load.db", Arc::new(SqliteDialect)).unwrap();
            let conn = db.connect().unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.close().unwrap();
        }

        // Reopening the database parses the stored schema row for `t`
        // through the dialect.
        let dialect = Arc::new(TestDialect::default());
        let db = open_db(&io, "dialect-load.db", dialect.clone()).unwrap();
        assert!(dialect.parse_calls.load(Ordering::SeqCst) >= 1);

        // DDL reparses the schema via the ParseSchema opcode, again through
        // the dialect.
        let conn = db.connect().unwrap();
        let before = dialect.parse_calls.load(Ordering::SeqCst);
        conn.execute("CREATE TABLE u (y INTEGER)").unwrap();
        assert!(dialect.parse_calls.load(Ordering::SeqCst) > before);

        // Both tables are usable under the dialect.
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO u VALUES (2)").unwrap();
        conn.close().unwrap();
    }

    #[test]
    fn registry_rejects_dialect_mismatch() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let _db = open_db(&io, "dialect-mismatch.db", Arc::new(SqliteDialect)).unwrap();

        let err =
            open_db(&io, "dialect-mismatch.db", Arc::new(TestDialect::default())).unwrap_err();
        assert!(
            err.to_string().contains("already open with dialect"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn registry_rejects_default_open_of_dialect_database() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let _db = open_db(
            &io,
            "dialect-mismatch-reverse.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();

        let err = open_db(&io, "dialect-mismatch-reverse.db", Arc::new(SqliteDialect)).unwrap_err();
        assert!(
            err.to_string().contains("already open with dialect"),
            "unexpected error: {err}"
        );
    }

    #[cfg(feature = "fs")]
    #[test]
    fn shared_memory_registry_rejects_dialect_mismatch() {
        let name = "dialect-shared-memory-mismatch";
        let _db = Database::open_shared_memory(name, Arc::new(SqliteDialect)).unwrap();

        let err = Database::open_shared_memory(name, Arc::new(TestDialect::default())).unwrap_err();
        assert!(
            err.to_string().contains("already open with dialect"),
            "unexpected error: {err}"
        );
    }
}
