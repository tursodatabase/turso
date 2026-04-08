/// Reproducer tests for savepoint FK mismatch issue #6285
///
/// The fuzz test `named_savepoint_differential_fuzz` fails with seed 1775525719316:
/// - step: 717
/// - stmt: RELEASE x
/// - sqlite: Err(ForeignKeyConstraint)
/// - turso: Ok([])
///
/// These tests explore various savepoint/FK interaction patterns.
/// They all pass, indicating the basic FK mechanics work correctly.
/// The actual bug requires ~717 specific operations to trigger.
///
/// To reproduce the exact failure:
/// ```
/// SEED=1775525719316 cargo test -p core_tester --test fuzz_tests named_savepoint_differential_fuzz
/// ```

#[cfg(test)]
mod savepoint_repro_tests {
    use core_tester::common::{limbo_exec_rows_fallible, TempDatabase};
    use rusqlite::params;
    use std::sync::Arc;

    fn run_both(
        db: &TempDatabase,
        limbo: &Arc<turso_core::Connection>,
        sqlite: &rusqlite::Connection,
        stmt: &str,
    ) -> (Result<(), String>, Result<(), String>) {
        let sqlite_res = sqlite
            .execute(stmt, params![])
            .map(|_| ())
            .map_err(|e| format!("{e:?}"));
        let limbo_res = limbo_exec_rows_fallible(db, limbo, stmt)
            .map(|_| ())
            .map_err(|e| e.to_string());
        (sqlite_res, limbo_res)
    }

    fn assert_same_outcome(
        stmt: &str,
        sqlite_res: &Result<(), String>,
        limbo_res: &Result<(), String>,
    ) {
        assert_eq!(
            sqlite_res.is_ok(),
            limbo_res.is_ok(),
            "outcome mismatch for `{stmt}`\nsqlite: {sqlite_res:?}\nlimbo: {limbo_res:?}"
        );
    }

    /// After a failed RELEASE, the savepoint should persist.
    /// This allows ROLLBACK TO to undo changes after FK error.
    #[turso_macros::test]
    pub fn failed_release_preserves_savepoint(db: TempDatabase) {
        let limbo = db.connect_limbo();
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();

        for schema in [
            "CREATE TABLE p (id INTEGER PRIMARY KEY)",
            "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        ] {
            limbo.execute(schema).unwrap();
            sqlite.execute(schema, params![]).unwrap();
        }

        limbo.execute("PRAGMA foreign_keys = ON").unwrap();
        sqlite
            .execute("PRAGMA foreign_keys = ON", params![])
            .unwrap();
        limbo.execute("INSERT INTO p(id) VALUES (1)").unwrap();
        sqlite
            .execute("INSERT INTO p(id) VALUES (1)", params![])
            .unwrap();
        limbo
            .execute("INSERT INTO c(id, pid) VALUES (1, 1)")
            .unwrap();
        sqlite
            .execute("INSERT INTO c(id, pid) VALUES (1, 1)", params![])
            .unwrap();

        for stmt in [
            "SAVEPOINT x",
            "DELETE FROM p WHERE id = 1",
            "RELEASE x", // should FAIL with FK error
        ] {
            let (s, l) = run_both(&db, &limbo, &sqlite, stmt);
            assert_same_outcome(stmt, &s, &l);
        }

        // After failed RELEASE, savepoint should still exist
        for stmt in ["ROLLBACK TO x", "RELEASE x"] {
            let (s, l) = run_both(&db, &limbo, &sqlite, stmt);
            assert_same_outcome(stmt, &s, &l);
        }
    }

    /// Nested savepoints persist after failed release of outer savepoint.
    #[turso_macros::test]
    pub fn nested_after_failed_release(db: TempDatabase) {
        let limbo = db.connect_limbo();
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();

        for schema in [
            "CREATE TABLE p (id INTEGER PRIMARY KEY)",
            "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        ] {
            limbo.execute(schema).unwrap();
            sqlite.execute(schema, params![]).unwrap();
        }

        limbo.execute("PRAGMA foreign_keys = ON").unwrap();
        sqlite
            .execute("PRAGMA foreign_keys = ON", params![])
            .unwrap();
        limbo.execute("INSERT INTO p(id) VALUES (1)").unwrap();
        sqlite
            .execute("INSERT INTO p(id) VALUES (1)", params![])
            .unwrap();
        limbo
            .execute("INSERT INTO c(id, pid) VALUES (1, 1)")
            .unwrap();
        sqlite
            .execute("INSERT INTO c(id, pid) VALUES (1, 1)", params![])
            .unwrap();

        for stmt in [
            "SAVEPOINT alpha",
            "SAVEPOINT y",
            "DELETE FROM p WHERE id = 1",
            "RELEASE alpha", // should FAIL (FK error)
        ] {
            let (s, l) = run_both(&db, &limbo, &sqlite, stmt);
            assert_same_outcome(stmt, &s, &l);
        }

        // Both y and alpha should still exist
        for stmt in ["RELEASE y", "ROLLBACK TO alpha", "RELEASE alpha"] {
            let (s, l) = run_both(&db, &limbo, &sqlite, stmt);
            assert_same_outcome(stmt, &s, &l);
        }
    }

    /// Same-name savepoint: release inner succeeds, release outer fails with FK error.
    #[turso_macros::test]
    pub fn same_name_release_failure(db: TempDatabase) {
        let limbo = db.connect_limbo();
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();

        for schema in [
            "CREATE TABLE p (id INTEGER PRIMARY KEY)",
            "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        ] {
            limbo.execute(schema).unwrap();
            sqlite.execute(schema, params![]).unwrap();
        }

        limbo.execute("PRAGMA foreign_keys = ON").unwrap();
        sqlite
            .execute("PRAGMA foreign_keys = ON", params![])
            .unwrap();
        limbo.execute("INSERT INTO p(id) VALUES (1)").unwrap();
        sqlite
            .execute("INSERT INTO p(id) VALUES (1)", params![])
            .unwrap();
        limbo
            .execute("INSERT INTO c(id, pid) VALUES (1, 1)")
            .unwrap();
        sqlite
            .execute("INSERT INTO c(id, pid) VALUES (1, 1)", params![])
            .unwrap();

        for stmt in [
            "SAVEPOINT x",                  // outer x
            "DELETE FROM p WHERE id = 1",   // FK violation
            "SAVEPOINT x",                  // inner x (same name)
            "INSERT INTO p(id) VALUES (1)", // fix FK
            "RELEASE x",                    // release inner x - should succeed
        ] {
            let (s, l) = run_both(&db, &limbo, &sqlite, stmt);
            assert_same_outcome(stmt, &s, &l);
        }

        // Outer x should fail with FK error (parent row deleted)
        let stmt = "RELEASE x";
        let (s, l) = run_both(&db, &limbo, &sqlite, stmt);
        assert_same_outcome(stmt, &s, &l);
    }
}
