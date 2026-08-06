#[cfg(test)]
mod tests {
    use crate::common::{ExecRows, TempDatabase};
    use tempfile::TempDir;

    /// Materialized views are still behind an experimental flag.
    fn opts() -> turso_core::DatabaseOpts {
        turso_core::DatabaseOpts::new().with_views(true)
    }

    /// A materialized view stores the query it was built from, and reloading
    /// the schema re-resolves it. Dropping something it reads therefore has
    /// to be refused: allowing it leaves a view whose query cannot resolve,
    /// which makes the whole database unopenable rather than just breaking
    /// that view. PostgreSQL refuses the same drops, and asks for CASCADE.
    #[test]
    fn dropping_a_table_a_materialized_view_reads_is_refused() {
        let path = TempDir::new().unwrap().keep().join("matview_dep_table.db");

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

            let err = conn.execute("DROP TABLE t").expect_err("must be refused");
            assert_eq!(
                err.to_string(),
                "Parse error: cannot drop table t because other objects depend on it"
            );

            // Dropping the view first frees the table.
            conn.execute("DROP VIEW tm").unwrap();
            conn.execute("DROP TABLE t").unwrap();
            conn.close().unwrap();
        }

        {
            let db = TempDatabase::new_with_existent_with_opts(&path, opts());
            let conn = db.connect_limbo();
            let names: Vec<(String,)> = conn.exec_rows("SELECT name FROM sqlite_schema");
            assert!(
                names.is_empty(),
                "both objects should be gone, found {names:?}"
            );
            conn.close().unwrap();
        }
    }

    /// A materialized view reading another one is refused outright. Building
    /// it would produce a view that never updates: refreshing the inner view
    /// does not carry on to the outer one, so it stays empty and every query
    /// against it silently returns nothing.
    #[test]
    fn a_materialized_view_reading_another_one_is_refused() {
        let path = TempDir::new().unwrap().keep().join("matview_nested.db");

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

            let err = conn
                .execute("CREATE MATERIALIZED VIEW tmm AS SELECT kind FROM tm")
                .expect_err("must be refused");
            assert_eq!(
                err.to_string(),
                "Parse error: materialized view tmm cannot read materialized view tm: \
                 nested materialized views are not supported"
            );
            conn.close().unwrap();
        }

        {
            // The refused statement left nothing behind.
            let db = TempDatabase::new_with_existent_with_opts(&path, opts());
            let conn = db.connect_limbo();
            let names: Vec<(String,)> =
                conn.exec_rows("SELECT name FROM sqlite_schema WHERE name = 'tmm'");
            assert!(names.is_empty(), "found {names:?}");
            conn.execute("INSERT INTO t VALUES ('a', 1)").unwrap();
            let rows: Vec<(String,)> = conn.exec_rows("SELECT kind FROM tm");
            assert_eq!(rows, vec![("a".to_string(),)]);
            conn.close().unwrap();
        }
    }

    /// A materialized view that nothing reads is still droppable, and a
    /// table only a regular view reads is not affected — a regular view's
    /// SQL is re-parsed lazily, so a dangling one does not stop the database
    /// from opening.
    #[test]
    fn drops_with_nothing_depending_on_them_still_work() {
        let path = TempDir::new().unwrap().keep().join("matview_dep_free.db");

        let db = TempDatabase::new_with_existent_with_opts(&path, opts());
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE t (kind TEXT, amt INTEGER)")
            .unwrap();
        conn.execute(
            "CREATE MATERIALIZED VIEW tm AS \
             SELECT kind, sum(amt) AS total FROM t GROUP BY kind",
        )
        .unwrap();
        conn.execute("DROP VIEW tm").unwrap();
        conn.execute("DROP TABLE t").unwrap();

        conn.execute("CREATE TABLE u (a INTEGER)").unwrap();
        conn.execute("CREATE VIEW uv AS SELECT a FROM u").unwrap();
        conn.execute("DROP TABLE u").unwrap();
        conn.close().unwrap();
    }
}
