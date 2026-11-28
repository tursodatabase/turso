//! Run with `cargo run -p turso_sqlite --example subscribe_changes`.

use std::sync::Arc;
use turso_core::{ChangeFilter, ConnectOpts, Database, MemoryIO, Result, SessionRole, Value};
use turso_sqlite::SqliteDialect;

fn main() -> Result<()> {
    let db = Database::open_file(
        Arc::new(MemoryIO::new()),
        ":memory:",
        Arc::new(SqliteDialect),
    )?;
    let reader = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    let writer = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    writer.execute(
        "CREATE TABLE members(org INTEGER, name TEXT, role TEXT, PRIMARY KEY(org, name))",
    )?;
    writer.execute("INSERT INTO members VALUES (7, 'sam', 'reader'), (8, 'sam', 'reader')")?;

    let member = db.subscribe_changes(ChangeFilter::Row {
        table: "members".into(),
        primary_key: vec![Value::from_i64(7), Value::build_text("sam")],
    });
    let show_role = || {
        reader
            .prepare("SELECT role FROM members WHERE org = 7 AND name = 'sam'")?
            .run_with_row_callback(|row| {
                println!("Sam's role in organization 7 is {}", row.get_value(0));
                Ok(())
            })
    };
    show_role()?;

    writer.execute("UPDATE members SET role = 'owner' WHERE org = 8 AND name = 'sam'")?;
    assert!(member.try_recv().is_none());
    writer.execute("BEGIN")?;
    writer.execute("UPDATE members SET role = 'editor' WHERE org = 7 AND name = 'sam'")?;
    assert!(member.try_recv().is_none());
    writer.execute("COMMIT")?;

    // A precise row notification and a full refresh both mean we should reread this row.
    if member.recv().is_some() {
        show_role()?;
    }
    Ok(())
}
