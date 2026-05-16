use std::sync::Arc;

use anyhow::Result;
use turso_core::{Connection, Database, DatabaseOpts, IO, OpenFlags, StepResult};

use crate::runner::memory::io::MemorySimIO;

const NON_UTF8_BLOB_HEX: &str = "F9CDA9E65FE48B3624615260C9C6AF6C067640E206DB5EECB7DBDE509158B035F62278A6A0C7A8A1523703096EE745FC946536BD1D";

fn make_conn(seed: u64) -> Result<(Arc<Connection>, Arc<MemorySimIO>)> {
    let io = Arc::new(MemorySimIO::new(seed, 4096, 100, 1, 5));
    let path = format!("sim_cast_blob_{seed}.db");
    let db = Database::open_file_with_flags(
        io.clone() as Arc<dyn IO>,
        &path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    Ok((db.connect()?, io))
}

fn query_blob_hex(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<String> {
    let mut stmt = conn.prepare("SELECT hex(dst) FROM t")?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                return Ok(row.get::<String>(0).expect("hex column should exist"));
            }
            StepResult::Done => panic!("hex query ended without a row"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

#[test]
fn sim_cast_blob_to_blob_preserves_non_utf8_bytes_in_update() -> Result<()> {
    let (conn, io) = make_conn(301)?;
    conn.execute("CREATE TABLE t(src BLOB, dst BLOB)")?;
    conn.execute(&format!(
        "INSERT INTO t(src) VALUES (X'{NON_UTF8_BLOB_HEX}')"
    ))?;

    conn.execute("UPDATE t SET dst = CAST(src AS BLOB)")?;

    assert_eq!(query_blob_hex(&conn, io.as_ref())?, NON_UTF8_BLOB_HEX);
    Ok(())
}
