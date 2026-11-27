//! A stored table with an overlay (`OpenOptions::table_overlay`): the
//! statements of the user read the overlay where they name the table, and
//! the statements of the engine read and write the stored table.

use turso_core::native_ext::*;
use turso_core::security::roles::SessionRole;
use turso_core::sync::Arc;
use turso_core::types::IOResultOr;
use turso_core::{
    ConnectOpts, Connection, Database, IOResult, LimboError, MemoryIO, OpenOptions, Result,
    Statement, Value,
};
use turso_sqlite::SqliteDialect;

#[test]
fn a_user_statement_reads_the_overlay_and_the_engine_reads_the_stored_table() {
    let conn = connection();
    conn.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    insert_as_engine(&conn, "INSERT INTO notes VALUES (1, 'stored')");

    // The overlay gives its built-in row, and then the rows that its cursor
    // reads from the stored table with a statement of the engine. If that
    // statement read the overlay, it would never end.
    assert_eq!(
        query(&conn, "SELECT id, body, source FROM notes ORDER BY id"),
        [
            row(0, "built-in", "overlay"),
            row(1, "stored", "stored table")
        ]
    );
    assert_eq!(
        query(&conn, "SELECT body FROM main.notes WHERE id = 0"),
        [vec![Value::from_text("built-in")]]
    );
    // A view reads the overlay too, because a statement of the user
    // expands it.
    conn.execute("CREATE VIEW note_bodies AS SELECT body FROM notes")
        .unwrap();
    assert_eq!(
        query(&conn, "SELECT count(*) FROM note_bodies"),
        [vec![Value::from_i64(2)]]
    );

    // The statement reads the overlay, so it cannot choose an index of the
    // stored table.
    conn.execute("CREATE INDEX notes_body ON notes (body)")
        .unwrap();
    for sql in [
        "SELECT * FROM notes INDEXED BY notes_body",
        "SELECT * FROM notes NOT INDEXED",
    ] {
        let error = conn.prepare(sql).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot be used on notes, which has an overlay"),
            "{sql}: {error}"
        );
    }

    // `SELECT *` gives the columns of the stored table, without the hidden
    // column of the overlay.
    assert_eq!(
        query(&conn, "SELECT * FROM notes ORDER BY id"),
        [
            vec![Value::from_i64(0), Value::from_text("built-in")],
            vec![Value::from_i64(1), Value::from_text("stored")]
        ]
    );

    // A statement of the engine reads the stored table, which has no
    // column `source`.
    let mut engine = conn.prepare_internal("SELECT id, body FROM notes").unwrap();
    assert_eq!(
        engine.run_collect_rows().unwrap(),
        [vec![Value::from_i64(1), Value::from_text("stored")]]
    );
    assert!(conn.prepare_internal("SELECT source FROM notes").is_err());
}

#[test]
fn the_overlay_applies_only_while_the_stored_table_exists() {
    let conn = connection();
    let error = conn.prepare("SELECT * FROM notes").unwrap_err();
    assert!(error.to_string().contains("no such table"), "{error}");

    // A temporary table with the name is another table, without overlay.
    conn.execute("CREATE TEMP TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("INSERT INTO temp.notes VALUES (5, 'temporary')")
        .unwrap();
    assert_eq!(
        query(&conn, "SELECT id, body FROM notes"),
        [vec![Value::from_i64(5), Value::from_text("temporary")]]
    );
    conn.execute("DROP TABLE temp.notes").unwrap();

    conn.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    assert_eq!(
        query(&conn, "SELECT id, body, source FROM notes"),
        [row(0, "built-in", "overlay")]
    );
}

/// The overlay changes where the rows come from, not the columns, so a
/// stored table with other columns than its overlay is not read through it.
#[test]
fn a_stored_table_without_the_columns_of_its_overlay_is_refused() {
    let conn = connection();
    conn.execute("CREATE TABLE notes (body TEXT, id INTEGER PRIMARY KEY)")
        .unwrap();
    let error = conn.prepare("SELECT body FROM notes").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("table notes does not have the columns of its overlay"),
        "{error}"
    );
}

#[test]
fn a_user_statement_does_not_write_a_table_with_an_overlay() {
    let conn = connection();
    conn.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    for sql in [
        "INSERT INTO notes VALUES (2, 'mine')",
        "UPDATE notes SET body = 'mine'",
        "DELETE FROM notes",
    ] {
        let error = conn.execute(sql).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("table notes may not be modified"),
            "{sql}: {error}"
        );
    }
    // A trigger that a statement of the user fires is part of that
    // statement, so it does not write the table either.
    conn.execute("CREATE TABLE events (id INTEGER)").unwrap();
    conn.execute(
        "CREATE TRIGGER copy AFTER INSERT ON events \
         BEGIN INSERT INTO notes VALUES (new.id, 'copied'); END",
    )
    .unwrap();
    let error = conn.execute("INSERT INTO events VALUES (3)").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("table notes may not be modified"),
        "{error}"
    );

    // A statement of the engine writes the stored table.
    insert_as_engine(&conn, "INSERT INTO notes VALUES (2, 'written')");
    assert_eq!(
        query(&conn, "SELECT id, body, source FROM notes ORDER BY id"),
        [
            row(0, "built-in", "overlay"),
            row(2, "written", "stored table")
        ]
    );
}

fn connection() -> Arc<Connection> {
    let options = OpenOptions::new(Arc::new(SqliteDialect)).table_overlay("notes", NotesOverlay);
    Database::open(Arc::new(MemoryIO::new()), ":memory:", options)
        .unwrap()
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap()
}

fn insert_as_engine(conn: &Arc<Connection>, sql: &str) {
    conn.prepare_internal(sql)
        .unwrap()
        .run_ignore_rows()
        .unwrap();
}

fn query(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

fn row(id: i64, body: &str, source: &str) -> Vec<Value> {
    vec![
        Value::from_i64(id),
        Value::from_text(body.to_string()),
        Value::from_text(source.to_string()),
    ]
}

/// The overlay of `notes`: one built-in row, and the rows of the stored
/// table. It has the columns of the stored table, and the hidden column
/// `source` says where a row comes from.
#[derive(Debug, Clone, Copy)]
struct NotesOverlay;

impl VirtualTableModule for NotesOverlay {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok("CREATE TABLE x(id INTEGER, body TEXT, source TEXT HIDDEN)".into())
    }

    fn create(&self, _args: &[Value]) -> Result<Self::Table> {
        Ok(*self)
    }
}

impl VirtualTable for NotesOverlay {
    type Cursor = NotesCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(NotesCursor {
            conn,
            statement: None,
            rows: Vec::new(),
            position: 0,
        })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> std::result::Result<IndexInfo, turso_ext::ResultCode> {
        Ok(IndexInfo {
            constraint_usages: vec![
                ConstraintUsage {
                    argv_index: None,
                    omit: false,
                };
                constraints.len()
            ],
            ..Default::default()
        })
    }
}

struct NotesCursor {
    conn: Arc<Connection>,
    /// The statement of the engine that reads the stored table, while
    /// `filter` reads it.
    statement: Option<Statement>,
    /// The rows of the overlay, without the column `source`, and whether
    /// each one is the built-in row.
    rows: Vec<(Value, Value, bool)>,
    position: usize,
}

impl VirtualTableCursor for NotesCursor {
    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> IOResultOr<bool> {
        if self.statement.is_none() {
            self.rows = vec![(Value::from_i64(0), Value::from_text("built-in"), true)];
            self.position = 0;
            self.statement = Some(self.conn.prepare_internal("SELECT id, body FROM notes")?);
        }
        let rows = &mut self.rows;
        turso_core::return_if_io!(self
            .statement
            .as_mut()
            .expect("the statement is prepared")
            .run_with_row_callback_nonblock(|row| {
                rows.push((row.get_value(0).clone(), row.get_value(1).clone(), false));
                Ok(())
            }));
        self.statement = None;
        Ok(IOResult::Done(!self.rows.is_empty()))
    }

    fn next(&mut self) -> IOResultOr<bool> {
        self.position += 1;
        Ok(IOResult::Done(self.position < self.rows.len()))
    }

    fn column(&mut self, column: usize) -> IOResultOr<Value> {
        let Some((id, body, builtin)) = self.rows.get(self.position) else {
            return Err(LimboError::InternalError("the cursor has no row".into()).into());
        };
        Ok(IOResult::Done(match column {
            0 => id.clone(),
            1 => body.clone(),
            _ if *builtin => Value::from_text("overlay"),
            _ => Value::from_text("stored table"),
        }))
    }

    fn rowid(&self) -> i64 {
        self.position as i64
    }
}
