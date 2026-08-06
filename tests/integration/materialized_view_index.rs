#[cfg(test)]
mod tests {
    use crate::common::{ExecRows, TempDatabase};
    use tempfile::TempDir;

    /// Materialized views are still behind an experimental flag.
    fn opts() -> turso_core::DatabaseOpts {
        turso_core::DatabaseOpts::new().with_views(true)
    }

    /// A materialized view is backed by a real btree, so it can carry
    /// indexes like a table. Reloading the schema builds indexes before it
    /// registers materialized views, so an index on one used to look like an
    /// index on a table that does not exist — and the whole database was
    /// rejected as corrupt, not just the view.
    #[test]
    fn index_on_a_materialized_view_survives_reopen() {
        let path = TempDir::new().unwrap().keep().join("matview_index.db");

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts());
            let conn = db.connect_limbo();
            conn.execute("CREATE TABLE t (kind TEXT, amt INTEGER)")
                .unwrap();
            conn.execute(
                "CREATE MATERIALIZED VIEW tm AS \
                 SELECT kind, sum(amt) AS total FROM t GROUP BY kind",
            )
            .unwrap();
            conn.execute("INSERT INTO t VALUES ('a', 1), ('b', 2), ('a', 3)")
                .unwrap();
            conn.execute("CREATE UNIQUE INDEX tm_kind ON tm (kind)")
                .unwrap();
            conn.close().unwrap();
        }

        {
            // Opening at all is most of the point: this used to fail with
            // "sqlite_schema contains index for missing table 'tm'".
            let db = TempDatabase::new_with_existent_with_opts(&path, opts());
            let conn = db.connect_limbo();

            let mut kinds: Vec<(String,)> = conn.exec_rows("SELECT kind FROM tm");
            kinds.sort();
            assert_eq!(
                kinds,
                vec![("a".to_string(),), ("b".to_string(),)],
                "the view must still be readable after reopening"
            );

            // The index came back too, not just the view.
            let indexes: Vec<(String,)> = conn.exec_rows(
                "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'tm'",
            );
            assert_eq!(indexes, vec![("tm_kind".to_string(),)]);

            conn.close().unwrap();
        }
    }

    /// Dropping the view has to take its indexes with it. Leaving a schema
    /// row behind makes the database unreadable on the next open, and
    /// leaving the btree behind leaks its pages.
    #[test]
    fn dropping_a_materialized_view_takes_its_indexes_with_it() {
        let path = TempDir::new().unwrap().keep().join("matview_index_drop.db");

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts());
            let conn = db.connect_limbo();
            conn.execute("CREATE TABLE t (kind TEXT, amt INTEGER)")
                .unwrap();
            conn.execute(
                "CREATE MATERIALIZED VIEW tm AS \
                 SELECT kind, sum(amt) AS total FROM t GROUP BY kind",
            )
            .unwrap();
            conn.execute("INSERT INTO t VALUES ('a', 1)").unwrap();
            conn.execute("CREATE UNIQUE INDEX tm_kind ON tm (kind)")
                .unwrap();
            conn.execute("CREATE INDEX tm_total ON tm (total)").unwrap();
            conn.execute("DROP VIEW tm").unwrap();
            conn.close().unwrap();
        }

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts());
            let conn = db.connect_limbo();
            let leftovers: Vec<(String,)> = conn
                .exec_rows("SELECT name FROM sqlite_schema WHERE tbl_name = 'tm' ORDER BY name");
            assert!(
                leftovers.is_empty(),
                "dropping the view must leave no schema rows behind, found {leftovers:?}"
            );
            // The base table is untouched, and the database is still usable.
            let rows: Vec<(String, i64)> = conn.exec_rows("SELECT kind, amt FROM t");
            assert_eq!(rows, vec![("a".to_string(), 1)]);
            let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
            assert_eq!(
                integrity,
                vec![("ok".to_string(),)],
                "the dropped view's index pages must not leak"
            );
            conn.close().unwrap();
        }
    }
}
