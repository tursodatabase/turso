use std::sync::Arc;

use crate::common::TempDatabase;
use parking_lot::RwLock;
use turso_core::Value;
use turso_pg::PgConnection;

#[test]
fn enabled_row_security_is_visible_to_another_connection() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();
    conn1.execute("CREATE TABLE t (x int)").unwrap();
    assert!(tables_with_row_security(&conn2).is_empty());

    conn1
        .execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .unwrap();

    assert_eq!(tables_with_row_security(&conn2), vec!["t"]);
}

#[test]
fn disabled_row_security_is_visible_to_another_connection() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();
    conn1.execute("CREATE TABLE t (x int)").unwrap();
    conn1
        .execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .unwrap();
    assert_eq!(tables_with_row_security(&conn2), vec!["t"]);

    conn1
        .execute("ALTER TABLE t DISABLE ROW LEVEL SECURITY")
        .unwrap();

    assert!(tables_with_row_security(&conn2).is_empty());
}

#[test]
fn row_security_is_kept_after_reopening_the_database() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE enabled (x int)").unwrap();
    conn.execute("CREATE TABLE disabled (x int)").unwrap();
    conn.execute("CREATE TABLE renamed (x int)").unwrap();
    conn.execute("CREATE TABLE dropped (x int)").unwrap();
    for table in ["enabled", "disabled", "renamed", "dropped"] {
        conn.execute(format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"))
            .unwrap();
    }
    conn.execute("ALTER TABLE disabled DISABLE ROW LEVEL SECURITY")
        .unwrap();
    conn.execute("ALTER TABLE renamed RENAME TO new_name")
        .unwrap();
    conn.execute("DROP TABLE dropped").unwrap();
    conn.close().unwrap();
    drop(conn);

    let db = db.reopen();

    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE renamed (x int)").unwrap();
    conn.execute("CREATE TABLE dropped (x int)").unwrap();
    assert_eq!(tables_with_row_security(&conn), vec!["enabled", "new_name"]);
}

#[test]
fn policies_are_visible_to_another_connection_and_kept_after_reopening() {
    let db = TempDatabase::builder().build();
    let conn = connect_with_documents(&db);
    let alice = db.connect_postgres();
    alice.execute("SET ROLE alice").unwrap();
    assert!(rows(&alice, "SELECT id FROM documents").is_empty());

    conn.execute("CREATE POLICY first ON documents USING (id = 1)")
        .unwrap();
    conn.execute("CREATE POLICY second ON documents USING (id = 2)")
        .unwrap();
    conn.execute("DROP POLICY second ON documents").unwrap();

    assert_eq!(rows(&alice, "SELECT id FROM documents"), vec![vec!["1"]]);
    conn.close().unwrap();
    alice.close().unwrap();
    drop(conn);
    drop(alice);

    let db = db.reopen();
    let alice = db.connect_postgres();
    alice.execute("SET ROLE alice").unwrap();
    assert_eq!(rows(&alice, "SELECT id FROM documents"), vec![vec!["1"]]);
}

#[test]
fn policy_is_dropped_when_its_statement_rolls_back() {
    let db = TempDatabase::builder().build();
    let conn = connect_with_documents(&db);
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE POLICY everything ON documents USING (true)")
        .unwrap();
    conn.execute("ROLLBACK").unwrap();

    conn.execute("SET ROLE alice").unwrap();

    assert!(rows(&conn, "SELECT id FROM documents").is_empty());
}

#[test]
fn schema_is_read_again_after_enabling_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE t (x int)").unwrap();
    conn.execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .unwrap();

    conn.inner().force_reparse_schema_without_publish().unwrap();

    // Read the connection's own schema: a query would first adopt the
    // database's shared schema, which still has the table.
    let schema = conn.inner().current_schema();
    let public = schema.namespace_named("public").unwrap();
    assert!(conn
        .inner()
        .dialect()
        .table_rls_enabled(&schema, public, "t"));
}

#[test]
fn policies_are_read_again_while_a_role_is_set() {
    let db = TempDatabase::builder().build();
    let conn = connect_with_documents(&db);
    conn.execute("CREATE POLICY first ON documents USING (id = 1)")
        .unwrap();
    conn.execute("SET ROLE alice").unwrap();

    conn.inner().force_reparse_schema_without_publish().unwrap();

    assert_eq!(rows(&conn, "SELECT id FROM documents"), vec![vec!["1"]]);
}

#[test]
fn role_that_does_not_own_the_table_cannot_enable_row_security() {
    let db = TempDatabase::builder().build();
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("CREATE TABLE t (x int)").unwrap();
    conn.execute("SET ROLE alice").unwrap();

    let error = conn
        .execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .unwrap_err();

    assert_eq!(error.to_string(), "must be owner of table t");
    conn.execute("RESET ROLE").unwrap();
    assert!(tables_with_row_security(&conn).is_empty());
}

#[test]
fn role_sees_no_rows_of_a_table_with_row_security() {
    let db = TempDatabase::builder().build();
    let conn = connect_with_documents(&db);

    conn.execute("SET ROLE alice").unwrap();

    assert!(rows(&conn, "SELECT id FROM documents").is_empty());
    assert_eq!(
        rows(&conn, "SELECT count(*) FROM documents"),
        vec![vec!["0"]]
    );
    assert!(rows(&conn, "SELECT id FROM documents WHERE id = 1").is_empty());
    assert!(rows(&conn, "SELECT max(id) FROM documents")[0][0].is_empty());
}

#[test]
fn superuser_sees_every_row_of_a_table_with_row_security() {
    let db = TempDatabase::builder().build();
    let conn = connect_with_documents(&db);

    assert_eq!(
        rows(&conn, "SELECT id FROM documents ORDER BY id"),
        vec![vec!["1"], vec!["2"]]
    );
}

#[test]
fn role_sees_every_row_of_a_table_without_row_security() {
    let db = TempDatabase::builder().build();
    let conn = connect_with_documents(&db);
    conn.execute("CREATE TABLE notes (id int)").unwrap();
    conn.execute("INSERT INTO notes VALUES (1), (2)").unwrap();
    conn.execute("GRANT SELECT ON notes TO alice").unwrap();

    conn.execute("SET ROLE alice").unwrap();

    assert_eq!(
        rows(&conn, "SELECT id FROM notes ORDER BY id"),
        vec![vec!["1"], vec!["2"]]
    );
}

#[test]
fn role_sees_the_rows_again_after_row_security_is_disabled() {
    let db = TempDatabase::builder().build();
    let conn = connect_with_documents(&db);

    conn.execute("ALTER TABLE documents DISABLE ROW LEVEL SECURITY")
        .unwrap();
    conn.execute("SET ROLE alice").unwrap();

    assert_eq!(
        rows(&conn, "SELECT id FROM documents ORDER BY id"),
        vec![vec!["1"], vec!["2"]]
    );
}

#[test]
fn statement_prepared_before_row_security_is_enabled_sees_no_rows() {
    let db = TempDatabase::builder().build();
    let conn1 = db.connect_postgres();
    let conn2 = db.connect_postgres();
    conn1.execute("CREATE ROLE alice").unwrap();
    conn1.execute("CREATE TABLE documents (id int)").unwrap();
    conn1.execute("INSERT INTO documents VALUES (1)").unwrap();
    conn1.execute("GRANT SELECT ON documents TO alice").unwrap();
    conn2.execute("SET ROLE alice").unwrap();
    let mut select = conn2.prepare("SELECT id FROM documents").unwrap();

    conn1
        .execute("ALTER TABLE documents ENABLE ROW LEVEL SECURITY")
        .unwrap();

    assert!(select.run_collect_rows().unwrap().is_empty());
}

#[test]
fn statement_prepared_by_the_superuser_sees_no_rows_after_set_role() {
    let db = TempDatabase::builder().build();
    let conn = connect_with_documents(&db);
    let mut select = conn.prepare("SELECT id FROM documents").unwrap();
    assert_eq!(select.run_collect_rows().unwrap().len(), 2);
    select.reset().unwrap();

    conn.execute("SET ROLE alice").unwrap();

    assert!(select.run_collect_rows().unwrap().is_empty());
}

#[test]
fn left_join_shows_the_hidden_rows_as_nulls() {
    let db = TempDatabase::builder().build();
    let conn = connect_with_documents(&db);
    conn.execute("CREATE TABLE notes (id int)").unwrap();
    conn.execute("INSERT INTO notes VALUES (1), (2)").unwrap();
    conn.execute("GRANT SELECT ON notes TO alice").unwrap();

    conn.execute("SET ROLE alice").unwrap();

    assert_eq!(
        rows(
            &conn,
            "SELECT n.id, d.id FROM notes n LEFT JOIN documents d ON d.id = n.id ORDER BY n.id"
        ),
        vec![vec!["1", ""], vec!["2", ""]]
    );
    assert!(rows(
        &conn,
        "SELECT n.id FROM notes n JOIN documents d ON d.id = n.id"
    )
    .is_empty());
}

#[test]
fn subqueries_ctes_and_views_see_no_hidden_rows() {
    let db = TempDatabase::builder().build();
    let conn = connect_with_documents(&db);
    conn.execute("CREATE TABLE notes (id int)").unwrap();
    conn.execute("INSERT INTO notes VALUES (1), (2)").unwrap();
    conn.execute("GRANT SELECT ON notes TO alice").unwrap();
    conn.execute("CREATE VIEW document_ids AS SELECT id FROM documents")
        .unwrap();
    // A view reads its tables as its owner, so alice must own it for the
    // view to hide the rows.
    conn.execute("ALTER VIEW document_ids OWNER TO alice")
        .unwrap();

    conn.execute("SET ROLE alice").unwrap();

    for sql in [
        "SELECT id FROM notes WHERE id IN (SELECT id FROM documents)",
        "SELECT id FROM notes n WHERE EXISTS (SELECT 1 FROM documents d WHERE d.id = n.id)",
        "SELECT id FROM notes WHERE id = (SELECT max(id) FROM documents)",
        "WITH d AS (SELECT id FROM documents) SELECT id FROM d",
        "SELECT id FROM (SELECT id FROM documents) d",
        "SELECT id FROM document_ids",
        "SELECT id FROM documents UNION SELECT id FROM documents",
    ] {
        assert!(rows(&conn, sql).is_empty(), "{sql}");
    }
    assert_eq!(
        rows(
            &conn,
            "SELECT id FROM notes WHERE id NOT IN (SELECT id FROM documents) ORDER BY id"
        ),
        vec![vec!["1"], vec!["2"]]
    );
}

#[test]
fn right_and_full_join_show_no_hidden_rows() {
    let db = TempDatabase::builder().build();
    let conn = connect_with_documents(&db);
    conn.execute("CREATE TABLE notes (id int)").unwrap();
    conn.execute("INSERT INTO notes VALUES (1), (3)").unwrap();
    conn.execute("GRANT SELECT ON notes TO alice").unwrap();
    conn.execute("SET ROLE alice").unwrap();

    assert!(rows(
        &conn,
        "SELECT d.id FROM notes n RIGHT JOIN documents d ON d.id = n.id"
    )
    .is_empty());
    assert_eq!(
        rows(
            &conn,
            "SELECT n.id, d.id FROM documents d FULL JOIN notes n ON d.id = n.id ORDER BY n.id"
        ),
        vec![vec!["1", ""], vec!["3", ""]]
    );
}

#[test]
fn copy_to_stdout_as_role_sends_no_rows() {
    let db = TempDatabase::builder().build();
    let conn = connect_with_documents(&db);

    conn.execute("SET ROLE alice").unwrap();

    let mut copy = conn.prepare("COPY documents TO STDOUT").unwrap();
    assert!(copy.run_collect_rows().unwrap().is_empty());
}

/// A virtual table that shows the database file shows the rows row-level
/// security hides, so only a superuser may read it.
#[test]
fn role_cannot_read_a_superuser_only_virtual_table() {
    let db = TempDatabase::builder().build();
    let conn = connect_with_documents(&db);
    for superuser_only in [true, false] {
        conn.inner()
            .register_internal_vtab(PageReader { superuser_only })
            .unwrap();
    }
    assert!(rows(&conn, "SELECT * FROM raw_page_reader").is_empty());

    conn.execute("SET ROLE alice").unwrap();

    let error = conn.execute("SELECT * FROM raw_page_reader").unwrap_err();
    assert_eq!(
        error.to_string(),
        "permission denied for table raw_page_reader"
    );
    assert!(rows(&conn, "SELECT * FROM page_reader").is_empty());
}

/// A database with role alice and table documents, which has row-level
/// security and two rows.
fn connect_with_documents(db: &TempDatabase) -> PgConnection {
    let conn = db.connect_postgres();
    conn.execute("CREATE ROLE alice").unwrap();
    conn.execute("CREATE TABLE documents (id int PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO documents VALUES (1), (2)")
        .unwrap();
    conn.execute("ALTER TABLE documents ENABLE ROW LEVEL SECURITY")
        .unwrap();
    conn.execute("GRANT SELECT ON documents TO alice").unwrap();
    conn
}

fn rows(conn: &PgConnection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.query(sql).unwrap().unwrap();
    stmt.run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row.iter().map(|value| value.to_string()).collect())
        .collect()
}

fn tables_with_row_security(conn: &PgConnection) -> Vec<String> {
    let mut stmt = conn
        .query("SELECT relname FROM pg_class WHERE relrowsecurity ORDER BY relname")
        .unwrap()
        .unwrap();
    stmt.run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| row[0].to_string())
        .collect()
}

/// An empty virtual table that only a superuser may read when
/// `superuser_only` is set.
#[derive(Debug)]
struct PageReader {
    superuser_only: bool,
}

impl turso_core::InternalVirtualTable for PageReader {
    fn name(&self) -> String {
        if self.superuser_only {
            "raw_page_reader".to_string()
        } else {
            "page_reader".to_string()
        }
    }

    fn sql(&self) -> String {
        format!("CREATE TABLE {}(data BLOB)", self.name())
    }

    fn superuser_only(&self) -> bool {
        self.superuser_only
    }

    fn open(
        &self,
        _conn: Arc<turso_core::Connection>,
    ) -> turso_core::Result<Arc<RwLock<dyn turso_core::InternalVirtualTableCursor>>> {
        Ok(Arc::new(RwLock::new(EmptyCursor)))
    }

    fn best_index(
        &self,
        constraints: &[turso_ext::ConstraintInfo],
        _order_by: &[turso_ext::OrderByInfo],
    ) -> Result<turso_ext::IndexInfo, turso_ext::ResultCode> {
        Ok(turso_ext::IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1.0,
            estimated_rows: 0,
            constraint_usages: constraints
                .iter()
                .map(|_| turso_ext::ConstraintUsage {
                    argv_index: None,
                    omit: false,
                })
                .collect(),
        })
    }
}

#[derive(Debug)]
struct EmptyCursor;

impl turso_core::InternalVirtualTableCursor for EmptyCursor {
    fn next(&mut self) -> Result<bool, turso_core::LimboError> {
        Ok(false)
    }

    fn rowid(&self) -> i64 {
        0
    }

    fn column(&self, _column: usize) -> Result<Value, turso_core::LimboError> {
        unreachable!("the table has no rows")
    }

    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<String>,
        _idx_num: i32,
    ) -> Result<bool, turso_core::LimboError> {
        Ok(false)
    }
}
