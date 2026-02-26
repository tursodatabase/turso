#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use turso_core::authorizer::{action, SQLITE_DENY, SQLITE_IGNORE, SQLITE_OK};
    use turso_core::{Database, LimboError, MemoryIO, IO};

    fn open_memory_db() -> (Arc<dyn IO>, Arc<Database>, Arc<turso_core::Connection>) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:").unwrap();
        let conn = db.connect().unwrap();
        (io, db, conn)
    }

    #[test]
    fn test_authorizer_deny_select() {
        let (_io, _db, conn) = open_memory_db();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();

        // Set authorizer that denies SELECT
        conn.set_authorizer(Some(Box::new(|action_code, _, _, _, _| {
            if action_code == action::SQLITE_SELECT {
                SQLITE_DENY
            } else {
                SQLITE_OK
            }
        })));

        let result = conn.prepare("SELECT * FROM t");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, LimboError::AuthDenied(_)),
            "Expected AuthDenied, got: {err:?}"
        );
    }

    #[test]
    fn test_authorizer_deny_insert() {
        let (_io, _db, conn) = open_memory_db();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();

        // Set authorizer that denies INSERT
        conn.set_authorizer(Some(Box::new(|action_code, _, _, _, _| {
            if action_code == action::SQLITE_INSERT {
                SQLITE_DENY
            } else {
                SQLITE_OK
            }
        })));

        let result = conn.prepare("INSERT INTO t VALUES (1)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, LimboError::AuthDenied(_)),
            "Expected AuthDenied, got: {err:?}"
        );
    }

    #[test]
    fn test_authorizer_deny_delete() {
        let (_io, _db, conn) = open_memory_db();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();

        // Set authorizer that denies DELETE
        conn.set_authorizer(Some(Box::new(|action_code, _, _, _, _| {
            if action_code == action::SQLITE_DELETE {
                SQLITE_DENY
            } else {
                SQLITE_OK
            }
        })));

        let result = conn.prepare("DELETE FROM t WHERE x = 1");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, LimboError::AuthDenied(_)),
            "Expected AuthDenied, got: {err:?}"
        );
    }

    #[test]
    fn test_authorizer_deny_update() {
        let (_io, _db, conn) = open_memory_db();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();

        // Set authorizer that denies UPDATE
        conn.set_authorizer(Some(Box::new(|action_code, _, _, _, _| {
            if action_code == action::SQLITE_UPDATE {
                SQLITE_DENY
            } else {
                SQLITE_OK
            }
        })));

        let result = conn.prepare("UPDATE t SET x = 2 WHERE x = 1");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, LimboError::AuthDenied(_)),
            "Expected AuthDenied, got: {err:?}"
        );
    }

    #[test]
    fn test_authorizer_deny_create_table() {
        let (_io, _db, conn) = open_memory_db();

        conn.set_authorizer(Some(Box::new(|action_code, _, _, _, _| {
            if action_code == action::SQLITE_CREATE_TABLE {
                SQLITE_DENY
            } else {
                SQLITE_OK
            }
        })));

        let result = conn.prepare("CREATE TABLE t (x INTEGER)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, LimboError::AuthDenied(_)),
            "Expected AuthDenied, got: {err:?}"
        );
    }

    #[test]
    fn test_authorizer_deny_drop_table() {
        let (_io, _db, conn) = open_memory_db();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();

        conn.set_authorizer(Some(Box::new(|action_code, _, _, _, _| {
            if action_code == action::SQLITE_DROP_TABLE {
                SQLITE_DENY
            } else {
                SQLITE_OK
            }
        })));

        let result = conn.prepare("DROP TABLE t");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, LimboError::AuthDenied(_)),
            "Expected AuthDenied, got: {err:?}"
        );
    }

    #[test]
    fn test_authorizer_deny_pragma() {
        let (_io, _db, conn) = open_memory_db();

        conn.set_authorizer(Some(Box::new(|action_code, _, _, _, _| {
            if action_code == action::SQLITE_PRAGMA {
                SQLITE_DENY
            } else {
                SQLITE_OK
            }
        })));

        let result = conn.prepare("PRAGMA table_info('sqlite_schema')");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, LimboError::AuthDenied(_)),
            "Expected AuthDenied, got: {err:?}"
        );
    }

    #[test]
    fn test_authorizer_allow_all() {
        let (_io, _db, conn) = open_memory_db();

        // Authorizer that allows everything
        conn.set_authorizer(Some(Box::new(|_, _, _, _, _| SQLITE_OK)));

        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("UPDATE t SET x = 2").unwrap();
        conn.execute("DELETE FROM t").unwrap();
        conn.execute("DROP TABLE t").unwrap();
    }

    #[test]
    fn test_authorizer_receives_table_name() {
        let (_io, _db, conn) = open_memory_db();
        conn.execute("CREATE TABLE my_table (x INTEGER)").unwrap();

        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_clone = captured.clone();

        conn.set_authorizer(Some(Box::new(move |action_code, arg3, arg4, arg5, _| {
            captured_clone.lock().unwrap().push((
                action_code,
                arg3.map(String::from),
                arg4.map(String::from),
                arg5.map(String::from),
            ));
            SQLITE_OK
        })));

        let _stmt = conn.prepare("INSERT INTO my_table VALUES (42)").unwrap();

        let calls = captured.lock().unwrap();
        // Should have an INSERT action with table name "my_table"
        let insert_call = calls
            .iter()
            .find(|(code, _, _, _)| *code == action::SQLITE_INSERT);
        assert!(
            insert_call.is_some(),
            "Expected INSERT auth check, got: {calls:?}"
        );
        let (_, arg3, _, _) = insert_call.unwrap();
        assert_eq!(arg3.as_deref(), Some("my_table"));
    }

    #[test]
    fn test_authorizer_receives_update_table_name() {
        let (_io, _db, conn) = open_memory_db();
        conn.execute("CREATE TABLE scores (val INTEGER)").unwrap();

        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_clone = captured.clone();

        conn.set_authorizer(Some(Box::new(move |action_code, arg3, arg4, arg5, _| {
            captured_clone.lock().unwrap().push((
                action_code,
                arg3.map(String::from),
                arg4.map(String::from),
                arg5.map(String::from),
            ));
            SQLITE_OK
        })));

        let _stmt = conn.prepare("UPDATE scores SET val = 10").unwrap();

        let calls = captured.lock().unwrap();
        let update_call = calls
            .iter()
            .find(|(code, _, _, _)| *code == action::SQLITE_UPDATE);
        assert!(
            update_call.is_some(),
            "Expected UPDATE auth check, got: {calls:?}"
        );
        let (_, arg3, _, _) = update_call.unwrap();
        assert_eq!(arg3.as_deref(), Some("scores"));
    }

    #[test]
    fn test_authorizer_clear_removes_restriction() {
        let (_io, _db, conn) = open_memory_db();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();

        // First, deny INSERT
        conn.set_authorizer(Some(Box::new(|action_code, _, _, _, _| {
            if action_code == action::SQLITE_INSERT {
                SQLITE_DENY
            } else {
                SQLITE_OK
            }
        })));

        let result = conn.prepare("INSERT INTO t VALUES (1)");
        assert!(result.is_err());

        // Clear the authorizer
        conn.set_authorizer(None);

        // Now INSERT should work
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
    }

    #[test]
    fn test_authorizer_replace_callback() {
        let (_io, _db, conn) = open_memory_db();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();

        // Deny INSERT
        conn.set_authorizer(Some(Box::new(|action_code, _, _, _, _| {
            if action_code == action::SQLITE_INSERT {
                SQLITE_DENY
            } else {
                SQLITE_OK
            }
        })));

        let result = conn.prepare("INSERT INTO t VALUES (1)");
        assert!(result.is_err());

        // Replace with one that allows INSERT but denies SELECT
        conn.set_authorizer(Some(Box::new(|action_code, _, _, _, _| {
            if action_code == action::SQLITE_SELECT {
                SQLITE_DENY
            } else {
                SQLITE_OK
            }
        })));

        // INSERT should now work
        conn.execute("INSERT INTO t VALUES (1)").unwrap();

        // SELECT should be denied
        let result = conn.prepare("SELECT * FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn test_authorizer_deny_specific_table() {
        let (_io, _db, conn) = open_memory_db();
        conn.execute("CREATE TABLE allowed (x INTEGER)").unwrap();
        conn.execute("CREATE TABLE forbidden (x INTEGER)").unwrap();

        // Deny INSERT only on 'forbidden' table
        conn.set_authorizer(Some(Box::new(|action_code, arg3, _, _, _| {
            if action_code == action::SQLITE_INSERT && arg3 == Some("forbidden") {
                SQLITE_DENY
            } else {
                SQLITE_OK
            }
        })));

        // INSERT into 'allowed' should work
        conn.execute("INSERT INTO allowed VALUES (1)").unwrap();

        // INSERT into 'forbidden' should fail
        let result = conn.prepare("INSERT INTO forbidden VALUES (1)");
        assert!(result.is_err());
    }

    #[test]
    fn test_authorizer_deny_transaction() {
        let (_io, _db, conn) = open_memory_db();

        conn.set_authorizer(Some(Box::new(|action_code, _, _, _, _| {
            if action_code == action::SQLITE_TRANSACTION {
                SQLITE_DENY
            } else {
                SQLITE_OK
            }
        })));

        let result = conn.prepare("BEGIN");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, LimboError::AuthDenied(_)),
            "Expected AuthDenied, got: {err:?}"
        );
    }

    #[test]
    fn test_authorizer_ignore_returns_no_error() {
        let (_io, _db, conn) = open_memory_db();
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();

        // SQLITE_IGNORE should not cause an error for statement-level checks
        conn.set_authorizer(Some(Box::new(|action_code, _, _, _, _| {
            if action_code == action::SQLITE_SELECT {
                SQLITE_IGNORE
            } else {
                SQLITE_OK
            }
        })));

        // SQLITE_IGNORE on SELECT should not error
        let result = conn.prepare("SELECT * FROM t");
        assert!(result.is_ok(), "IGNORE should not cause error for SELECT");
    }
}
