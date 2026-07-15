//! End-to-end tests for the incremental blob I/O opcodes (BlobRead/BlobWrite), which
//! read and write bytes within a column value in place, following overflow pages.
//! Programs are hand-assembled with ProgramBuilder and run against a real table so the
//! whole faithful pager-backed path is exercised, including persistence.

use std::sync::Arc;

use crate::vdbe::builder::{CursorType, ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::insn::{Insn, RegisterOrLiteral};
use crate::{
    Connection, Database, DatabaseOpts, LimboError, MemoryIO, OpenFlags, Statement, Value, IO,
};

fn fresh_db() -> Arc<Connection> {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:").unwrap();
    db.connect().unwrap()
}

fn fresh_db_with_opts(opts: DatabaseOpts) -> Arc<Connection> {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(io, ":memory:", OpenFlags::Create, opts, None).unwrap();
    db.connect().unwrap()
}

fn fresh_db_with_generated_columns() -> Arc<Connection> {
    fresh_db_with_opts(DatabaseOpts::new().with_generated_columns(true))
}

fn exec(conn: &Arc<Connection>, sql: &str) {
    conn.execute(sql).unwrap();
}

fn table_root(conn: &Arc<Connection>, name: &str) -> i64 {
    conn.with_schema_mut(|s| s.get_btree_table(name).unwrap().root_page)
        .unwrap()
}

/// Build a write program that opens `t`, seeks rowid 1, runs `body` (which emits
/// BlobRead/BlobWrite against the open cursor), and run it, returning result rows.
fn run_blob_program(
    conn: &Arc<Connection>,
    root: i64,
    body: impl FnOnce(&mut ProgramBuilder, usize),
) -> Vec<Vec<Value>> {
    let table = conn
        .with_schema_mut(|s| s.get_btree_table("t").unwrap())
        .unwrap();
    let mut b = ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(1, 64, 8));
    b.prologue();
    b.begin_write_operation().unwrap();
    let cursor = b.alloc_cursor_id(CursorType::BTreeTable(table));
    b.emit_insn(Insn::OpenWrite {
        cursor_id: cursor,
        root_page: RegisterOrLiteral::Literal(root),
        db: 0,
    });
    let r_rowid = b.alloc_register();
    b.emit_insn(Insn::Integer {
        value: 1,
        dest: r_rowid,
    });
    let l_notfound = b.allocate_label();
    b.emit_insn(Insn::SeekRowid {
        cursor_id: cursor,
        src_reg: r_rowid,
        target_pc: l_notfound,
    });
    body(&mut b, cursor);
    let l_end = b.allocate_label();
    b.emit_insn(Insn::Goto { target_pc: l_end });
    b.preassign_label_to_next_insn(l_notfound);
    b.emit_halt_err(1, "row not found".to_string());
    b.preassign_label_to_next_insn(l_end);
    conn.with_schema_mut(|schema| b.epilogue(schema)).unwrap();
    let program = b.build(conn.clone(), false, "blob io test").unwrap();
    let mut stmt = Statement::new(program, conn.get_pager(), QueryMode::Normal, 0);
    stmt.run_collect_rows().unwrap()
}

fn blob_bytes(v: &Value) -> Vec<u8> {
    match v {
        Value::Blob(b) => b.to_vec(),
        other => panic!("expected blob, got {other:?}"),
    }
}

#[test]
fn blob_read_local_value() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(
        &conn,
        "INSERT INTO t VALUES (1, x'00112233445566778899aabbccddeeff')",
    );
    let root = table_root(&conn, "t");
    // Read 4 bytes at offset 3 of column 1 (data).
    let rows = run_blob_program(&conn, root, |b, cursor| {
        let r_off = b.alloc_register();
        let r_amt = b.alloc_register();
        let r_out = b.alloc_register();
        b.emit_insn(Insn::Integer {
            value: 3,
            dest: r_off,
        });
        b.emit_insn(Insn::Integer {
            value: 4,
            dest: r_amt,
        });
        b.emit_insn(Insn::BlobRead {
            cursor,
            column: 1,
            offset: r_off,
            amount: r_amt,
            dest: r_out,
        });
        b.emit_result_row(r_out, 1);
    });
    assert_eq!(blob_bytes(&rows[0][0]), vec![0x33, 0x44, 0x55, 0x66]);
}

#[test]
fn blob_write_local_persists() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(
        &conn,
        "INSERT INTO t VALUES (1, x'00000000000000000000000000000000')",
    );
    let root = table_root(&conn, "t");
    // Overwrite 3 bytes at offset 5 with 0xAA 0xBB 0xCC, then read them straight back.
    let rows = run_blob_program(&conn, root, |b, cursor| {
        let r_off = b.alloc_register();
        let r_src = b.alloc_register();
        let r_amt = b.alloc_register();
        let r_out = b.alloc_register();
        b.emit_insn(Insn::Integer {
            value: 5,
            dest: r_off,
        });
        b.emit_insn(Insn::Blob {
            value: crate::types::value_blob_from_slice(&[0xAA, 0xBB, 0xCC]),
            dest: r_src,
        });
        let r_ack = b.alloc_register();
        b.emit_insn(Insn::BlobWrite {
            cursor,
            column: 1,
            offset: r_off,
            src: r_src,
            dest: r_ack,
        });
        b.emit_insn(Insn::Integer {
            value: 3,
            dest: r_amt,
        });
        b.emit_insn(Insn::BlobRead {
            cursor,
            column: 1,
            offset: r_off,
            amount: r_amt,
            dest: r_out,
        });
        b.emit_result_row(r_out, 1);
    });
    assert_eq!(blob_bytes(&rows[0][0]), vec![0xAA, 0xBB, 0xCC]);
    // And the write persisted: a fresh SQL read sees it.
    let mut q = conn
        .query("SELECT substr(data, 6, 3) FROM t WHERE id = 1")
        .unwrap()
        .unwrap();
    let got = q.run_collect_rows().unwrap();
    assert_eq!(blob_bytes(&got[0][0]), vec![0xAA, 0xBB, 0xCC]);
}

#[test]
fn blob_read_write_across_overflow() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    // A blob far larger than a page forces overflow pages.
    exec(&conn, "INSERT INTO t VALUES (1, zeroblob(9000))");
    let root = table_root(&conn, "t");
    // Write a marker straddling the local/overflow boundary region (offset 3000, which
    // is well past the ~4KB local cap for a page, and spanning into deeper overflow).
    let pattern: Vec<u8> = (0u16..600).map(|i| (i % 251) as u8).collect();
    let pat_for_move = crate::types::value_blob_from_slice(&pattern);
    let rows = run_blob_program(&conn, root, move |b, cursor| {
        let r_off = b.alloc_register();
        let r_src = b.alloc_register();
        let r_amt = b.alloc_register();
        let r_out = b.alloc_register();
        b.emit_insn(Insn::Integer {
            value: 3000,
            dest: r_off,
        });
        b.emit_insn(Insn::Blob {
            value: pat_for_move,
            dest: r_src,
        });
        let r_ack = b.alloc_register();
        b.emit_insn(Insn::BlobWrite {
            cursor,
            column: 1,
            offset: r_off,
            src: r_src,
            dest: r_ack,
        });
        b.emit_insn(Insn::Integer {
            value: 600,
            dest: r_amt,
        });
        b.emit_insn(Insn::BlobRead {
            cursor,
            column: 1,
            offset: r_off,
            amount: r_amt,
            dest: r_out,
        });
        b.emit_result_row(r_out, 1);
    });
    assert_eq!(blob_bytes(&rows[0][0]), pattern);
    // Persisted across the overflow chain: SQL sees the same bytes.
    let mut q = conn
        .query("SELECT substr(data, 3001, 600) FROM t WHERE id = 1")
        .unwrap()
        .unwrap();
    let got = q.run_collect_rows().unwrap();
    assert_eq!(blob_bytes(&got[0][0]), pattern);
}

#[test]
fn connection_blob_api_round_trip() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "INSERT INTO t VALUES (1, zeroblob(20))");
    {
        let mut blob = conn.blob_open("t", "data", 1, true).unwrap();
        assert_eq!(blob.bytes(), 20);
        blob.write(4, &[0x11, 0x22, 0x33, 0x44]).unwrap();
        let mut buf = [0u8; 4];
        blob.read(4, &mut buf).unwrap();
        assert_eq!(buf, [0x11, 0x22, 0x33, 0x44]);
        blob.close().unwrap();
    }
    let mut q = conn
        .query("SELECT substr(data, 5, 4) FROM t WHERE id = 1")
        .unwrap()
        .unwrap();
    let got = q.run_collect_rows().unwrap();
    assert_eq!(blob_bytes(&got[0][0]), vec![0x11, 0x22, 0x33, 0x44]);
}

#[test]
fn connection_blob_api_overflow() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "INSERT INTO t VALUES (7, zeroblob(9000))");
    let pattern: Vec<u8> = (0u16..500).map(|i| (i % 251) as u8).collect();
    {
        let mut blob = conn.blob_open("t", "data", 7, true).unwrap();
        assert_eq!(blob.bytes(), 9000);
        blob.write(4000, &pattern).unwrap();
        let mut buf = vec![0u8; 500];
        blob.read(4000, &mut buf).unwrap();
        assert_eq!(buf, pattern);
        blob.close().unwrap();
    }
    let mut q = conn
        .query("SELECT substr(data, 4001, 500) FROM t WHERE id = 7")
        .unwrap()
        .unwrap();
    let got = q.run_collect_rows().unwrap();
    assert_eq!(blob_bytes(&got[0][0]), pattern);
}

// The expiry matrix below mirrors sqlite's e_blobopen.test (R-50542-62589): a handle
// expires when *its own row* is updated, deleted, or replaced; writes to other rows
// of the same table merely force a re-seek and the handle stays usable.

#[test]
fn blob_expires_on_own_row_update() {
    let conn = fresh_db();
    exec(
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, x, data BLOB)",
    );
    exec(&conn, "INSERT INTO t VALUES (1, 0, zeroblob(16))");
    let mut blob = conn.blob_open("t", "data", 1, false).unwrap();
    let mut buf = [0u8; 4];
    blob.read(0, &mut buf).unwrap();
    // Updating even a DIFFERENT column of the same row expires the handle
    // (e_blobopen 2.x: "UPDATE other column invalidates handle").
    exec(&conn, "UPDATE t SET x = 1 WHERE id = 1");
    assert!(matches!(
        blob.read(0, &mut buf),
        Err(LimboError::BlobHandleExpired)
    ));
    // Expiry is sticky.
    assert!(matches!(
        blob.read(0, &mut buf),
        Err(LimboError::BlobHandleExpired)
    ));
    // Closing an expired handle is not an error, like sqlite3_blob_close.
    blob.close().unwrap();
}

#[test]
fn blob_expires_on_own_row_delete() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "INSERT INTO t VALUES (1, zeroblob(16))");
    let mut blob = conn.blob_open("t", "data", 1, false).unwrap();
    exec(&conn, "DELETE FROM t WHERE id = 1");
    let mut buf = [0u8; 4];
    assert!(matches!(
        blob.read(0, &mut buf),
        Err(LimboError::BlobHandleExpired)
    ));
    blob.close().unwrap();
}

#[test]
fn blob_expires_on_own_row_replace() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "INSERT INTO t VALUES (1, zeroblob(16))");
    let mut blob = conn.blob_open("t", "data", 1, false).unwrap();
    let mut buf = [0u8; 2];
    blob.read(0, &mut buf).unwrap();
    exec(&conn, "INSERT OR REPLACE INTO t VALUES (1, zeroblob(8))");
    assert!(matches!(
        blob.read(0, &mut buf),
        Err(LimboError::BlobHandleExpired)
    ));
    blob.close().unwrap();
}

#[test]
fn blob_survives_writes_to_other_rows() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "INSERT INTO t VALUES (1, x'00112233')");
    exec(&conn, "INSERT INTO t VALUES (2, zeroblob(16))");
    let mut blob = conn.blob_open("t", "data", 1, false).unwrap();
    let mut buf = [0u8; 4];
    blob.read(0, &mut buf).unwrap();
    // Deleting, updating, and inserting OTHER rows disturbs the cursor but must
    // not expire the handle: the next access re-seeks the pinned rowid.
    exec(&conn, "DELETE FROM t WHERE id = 2");
    blob.read(0, &mut buf).unwrap();
    assert_eq!(buf, [0x00, 0x11, 0x22, 0x33]);
    exec(&conn, "INSERT INTO t VALUES (3, zeroblob(16))");
    exec(&conn, "UPDATE t SET data = zeroblob(4) WHERE id = 3");
    blob.read(0, &mut buf).unwrap();
    assert_eq!(buf, [0x00, 0x11, 0x22, 0x33]);
    blob.close().unwrap();
}

#[test]
fn blob_survives_splits_from_other_row_inserts() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "INSERT INTO t VALUES (1, x'a1b2c3d4')");
    let mut blob = conn.blob_open("t", "data", 1, false).unwrap();
    let mut buf = [0u8; 4];
    blob.read(0, &mut buf).unwrap();
    // Enough inserts to split the leaf several times and relocate row 1's cell;
    // the handle must follow it via re-seek and keep reading the right bytes.
    for i in 2..300 {
        exec(&conn, &format!("INSERT INTO t VALUES ({i}, zeroblob(100))"));
    }
    blob.read(0, &mut buf).unwrap();
    assert_eq!(buf, [0xa1, 0xb2, 0xc3, 0xd4]);
    blob.close().unwrap();
}

#[test]
fn blob_write_handle_blocks_other_writers_and_commits_on_close() {
    // Divergence from sqlite's e_blobwrite 3.x: in SQLite a same-connection write
    // statement can expire an open read-write handle, and writes made before the
    // expiry persist. Turso permits one write statement per connection at a time,
    // so while a read-write handle is open, other write statements fail with
    // StatementsInProgress instead — a write handle can never be expired out from
    // under its own connection. Its writes commit when the handle closes.
    let conn = fresh_db();
    exec(
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, x, data BLOB)",
    );
    exec(&conn, "INSERT INTO t VALUES (1, 0, zeroblob(8))");
    let mut blob = conn.blob_open("t", "data", 1, true).unwrap();
    blob.write(0, &[0xAA, 0xBB]).unwrap();
    assert!(matches!(
        conn.execute("UPDATE t SET x = 7 WHERE id = 1"),
        Err(LimboError::StatementsInProgress(_))
    ));
    blob.close().unwrap();
    // After close the write statement slot frees up and the blob write is durable.
    exec(&conn, "UPDATE t SET x = 7 WHERE id = 1");
    let mut q = conn
        .query("SELECT substr(data, 1, 2), x FROM t WHERE id = 1")
        .unwrap()
        .unwrap();
    let got = q.run_collect_rows().unwrap();
    assert_eq!(blob_bytes(&got[0][0]), vec![0xAA, 0xBB]);
    assert!(matches!(
        got[0][1],
        Value::Numeric(crate::Numeric::Integer(7))
    ));
}

#[test]
fn blob_survives_writes_to_other_tables() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "CREATE TABLE other(id INTEGER PRIMARY KEY, v)");
    exec(&conn, "INSERT INTO t VALUES (1, x'00112233')");
    let mut blob = conn.blob_open("t", "data", 1, false).unwrap();
    exec(&conn, "INSERT INTO other VALUES (1, 42)");
    let mut buf = [0u8; 4];
    blob.read(0, &mut buf).unwrap();
    assert_eq!(buf, [0x00, 0x11, 0x22, 0x33]);
    blob.close().unwrap();
}

#[test]
fn blob_open_rejects_write_on_indexed_column() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "CREATE INDEX t_data ON t(data)");
    exec(&conn, "INSERT INTO t VALUES (1, x'0011')");
    let err = conn.blob_open("t", "data", 1, true).unwrap_err();
    assert!(
        err.to_string().contains("indexed column"),
        "unexpected error: {err}"
    );
    // Read-only access to an indexed column is fine.
    let blob = conn.blob_open("t", "data", 1, false).unwrap();
    assert_eq!(blob.bytes(), 2);
    blob.close().unwrap();
}

#[test]
fn blob_open_rejects_write_on_unique_column() {
    let conn = fresh_db();
    exec(
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB UNIQUE)",
    );
    exec(&conn, "INSERT INTO t VALUES (1, x'0011')");
    let err = conn.blob_open("t", "data", 1, true).unwrap_err();
    assert!(
        err.to_string().contains("indexed column"),
        "unexpected error: {err}"
    );
}

#[test]
fn blob_open_foreign_key_column_write_gated_on_enforcement() {
    // e_blobopen R-50117-55204: a foreign-key child column refuses read-write
    // handles only while foreign key enforcement is on.
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE p(x TEXT PRIMARY KEY)");
    exec(
        &conn,
        "CREATE TABLE c(id INTEGER PRIMARY KEY, r TEXT REFERENCES p(x))",
    );
    exec(&conn, "INSERT INTO p VALUES ('ab')");
    exec(&conn, "INSERT INTO c VALUES (1, 'ab')");
    // Enforcement off (the default): read-write open succeeds.
    let blob = conn.blob_open("c", "r", 1, true).unwrap();
    blob.close().unwrap();
    conn.set_foreign_keys_enabled(true);
    let err = conn.blob_open("c", "r", 1, true).unwrap_err();
    assert!(
        err.to_string().contains("foreign key"),
        "unexpected error: {err}"
    );
    // Read-only open is fine regardless.
    let blob = conn.blob_open("c", "r", 1, false).unwrap();
    blob.close().unwrap();
}

#[test]
fn blob_open_rejects_non_text_blob_values() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, v)");
    exec(&conn, "INSERT INTO t VALUES (1, 42), (2, 3.5), (3, NULL)");
    for (rowid, type_name) in [(1, "integer"), (2, "real"), (3, "null")] {
        let err = conn.blob_open("t", "v", rowid, false).unwrap_err();
        assert!(
            err.to_string().contains(type_name),
            "rowid {rowid}: unexpected error: {err}"
        );
    }
}

#[test]
fn blob_open_rejects_rowid_alias() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "INSERT INTO t VALUES (1, x'00')");
    let err = conn.blob_open("t", "id", 1, false).unwrap_err();
    assert!(
        err.to_string().contains("integer"),
        "unexpected error: {err}"
    );
}

#[test]
fn blob_open_rejects_missing_objects() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "INSERT INTO t VALUES (1, x'00')");
    assert!(conn.blob_open("nope", "data", 1, false).is_err());
    assert!(conn.blob_open("t", "nope", 1, false).is_err());
    let err = conn.blob_open("t", "data", 999, false).unwrap_err();
    assert!(
        err.to_string().contains("no such rowid"),
        "unexpected error: {err}"
    );
}

#[test]
fn blob_text_value_reports_byte_length() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    // 'héllo' is 5 characters but 6 UTF-8 bytes; the blob API addresses bytes.
    exec(&conn, "INSERT INTO t VALUES (1, 'héllo')");
    let mut blob = conn.blob_open("t", "v", 1, false).unwrap();
    assert_eq!(blob.bytes(), "héllo".len());
    let mut buf = vec![0u8; blob.bytes()];
    blob.read(0, &mut buf).unwrap();
    assert_eq!(buf, "héllo".as_bytes());
    blob.close().unwrap();
}

#[test]
fn blob_range_and_readonly_errors() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "INSERT INTO t VALUES (1, zeroblob(8))");
    let mut blob = conn.blob_open("t", "data", 1, false).unwrap();
    let mut buf = [0xFFu8; 4];
    // In-range and boundary reads work; anything past the end errors without
    // killing the handle. A zeroblob() value reads back as zeroes (e_blobopen
    // R-58813-55036).
    blob.read(4, &mut buf).unwrap();
    assert_eq!(buf, [0, 0, 0, 0]);
    assert!(blob.read(5, &mut buf).is_err());
    blob.read(8, &mut []).unwrap();
    assert!(blob.read(9, &mut []).is_err());
    blob.read(0, &mut buf).unwrap();
    // Writing through a read-only handle is refused up front.
    assert!(matches!(blob.write(0, &[1]), Err(LimboError::ReadOnly)));
    blob.close().unwrap();

    let mut blob = conn.blob_open("t", "data", 1, true).unwrap();
    assert!(blob.write(5, &[0; 4]).is_err());
    blob.write(4, &[1, 2, 3, 4]).unwrap();
    blob.close().unwrap();
}

#[test]
fn blob_wide_row_header_spills_into_overflow() {
    let conn = fresh_db();
    // With 512-byte pages, a 600-column record header (> max local payload) cannot
    // fit in the leaf-local payload, so locating a column must stream header bytes
    // from the overflow chain.
    exec(&conn, "PRAGMA page_size = 512");
    let cols: Vec<String> = (0..600).map(|i| format!("c{i}")).collect();
    exec(
        &conn,
        &format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, {}, data BLOB)",
            cols.join(", ")
        ),
    );
    let ones = vec!["1"; 600].join(", ");
    exec(
        &conn,
        &format!("INSERT INTO t VALUES (1, {ones}, zeroblob(300))"),
    );
    let pattern: Vec<u8> = (0u16..64).map(|i| i as u8).collect();
    {
        let mut blob = conn.blob_open("t", "data", 1, true).unwrap();
        assert_eq!(blob.bytes(), 300);
        blob.write(100, &pattern).unwrap();
        let mut buf = vec![0u8; 64];
        blob.read(100, &mut buf).unwrap();
        assert_eq!(buf, pattern);
        blob.close().unwrap();
    }
    let mut q = conn
        .query("SELECT substr(data, 101, 64), c599 FROM t WHERE id = 1")
        .unwrap()
        .unwrap();
    let got = q.run_collect_rows().unwrap();
    assert_eq!(blob_bytes(&got[0][0]), pattern);
    // Neighboring columns were untouched.
    assert!(matches!(
        got[0][1],
        Value::Numeric(crate::Numeric::Integer(1))
    ));
}

#[test]
fn blob_open_rejects_views() {
    // incrblob3.test: "cannot open view" rather than "no such table".
    let conn = fresh_db_with_opts(DatabaseOpts::new().with_views(true));
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "INSERT INTO t VALUES (1, x'0011')");
    exec(&conn, "CREATE VIEW v AS SELECT data FROM t");
    let err = conn.blob_open("v", "data", 1, false).unwrap_err();
    assert!(
        err.to_string().contains("cannot open view"),
        "unexpected error: {err}"
    );
}

#[test]
fn blob_open_maps_around_virtual_generated_columns() {
    let conn = fresh_db_with_generated_columns();
    // The virtual column `g` occupies schema position 1 but has no record slot, so
    // `data` is schema position 2 yet record position 1.
    exec(
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, g INTEGER GENERATED ALWAYS AS (id + 1) VIRTUAL, data BLOB)",
    );
    exec(&conn, "INSERT INTO t(id, data) VALUES (1, zeroblob(8))");
    {
        let mut blob = conn.blob_open("t", "data", 1, true).unwrap();
        assert_eq!(blob.bytes(), 8);
        blob.write(0, &[0xAA, 0xBB]).unwrap();
        blob.close().unwrap();
    }
    let mut q = conn
        .query("SELECT substr(data, 1, 2), g FROM t WHERE id = 1")
        .unwrap()
        .unwrap();
    let got = q.run_collect_rows().unwrap();
    assert_eq!(blob_bytes(&got[0][0]), vec![0xAA, 0xBB]);
    assert!(matches!(
        got[0][1],
        Value::Numeric(crate::Numeric::Integer(2))
    ));
    // The generated column itself cannot be opened.
    let err = conn.blob_open("t", "g", 1, false).unwrap_err();
    assert!(
        err.to_string().contains("generated"),
        "unexpected error: {err}"
    );
}

#[test]
fn blob_overflow_page_survives_cache_eviction() {
    // Regression: the spatial-locality cache kept an unpinned PageRef to an overflow
    // page across blob operations. If the pager evicted that page between reads (it
    // takes the buffer out even while a PageRef is held), the next read at the same
    // overflow offset dereferenced a page with no buffer and panicked. Pinning the
    // cached overflow page keeps it evictable-proof, exactly like the cursor stack.
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "INSERT INTO t VALUES (1, zeroblob(50000))");
    // Seed a recognizable pattern deep in the overflow chain via SQL.
    let mut w = conn
        .query("UPDATE t SET data = substr(data, 1, 45000) || x'0102030405060708' || substr(data, 45009) WHERE id = 1")
        .unwrap()
        .unwrap();
    let _ = w.run_collect_rows().unwrap();
    drop(w);

    let mut blob = conn.blob_open("t", "data", 1, false).unwrap();
    let mut buf = [0u8; 8];
    // First read pins a deep overflow page as the spatial-locality page.
    blob.read(45000, &mut buf).unwrap();
    assert_eq!(buf, [1, 2, 3, 4, 5, 6, 7, 8]);

    // Evict every clean, unpinned page — what sustained cache pressure eventually
    // does. The blob's pinned overflow page must survive; everything else goes.
    conn.get_pager().test_evict_all_unpinned_clean();

    // Same offset again: hits the spatial-locality fast path. Must not panic and must
    // still return the right bytes.
    blob.read(45000, &mut buf).unwrap();
    assert_eq!(buf, [1, 2, 3, 4, 5, 6, 7, 8]);
    // A different offset (forces a fresh overflow fetch) also still works.
    let mut buf2 = [0u8; 4];
    blob.read(100, &mut buf2).unwrap();
    blob.close().unwrap();
}

#[test]
fn blob_overflow_pin_is_released_on_close() {
    // A closed (or dropped) blob handle must not leave any overflow page pinned;
    // otherwise that page could never be evicted again. After close, evicting all
    // clean unpinned pages must actually drop the formerly-pinned overflow page.
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    exec(&conn, "INSERT INTO t VALUES (1, zeroblob(50000))");
    {
        let mut blob = conn.blob_open("t", "data", 1, false).unwrap();
        let mut buf = [0u8; 8];
        blob.read(45000, &mut buf).unwrap();
        blob.close().unwrap();
    }
    // If the pin leaked, this eviction would silently skip the pinned page; the
    // subsequent full-table read still must succeed and see all 50000 bytes.
    conn.get_pager().test_evict_all_unpinned_clean();
    let mut q = conn
        .query("SELECT length(data) FROM t WHERE id = 1")
        .unwrap()
        .unwrap();
    let rows = q.run_collect_rows().unwrap();
    assert!(matches!(
        rows[0][0],
        Value::Numeric(crate::Numeric::Integer(50000))
    ));
}

/// Exhaustive boundary sweep of the read/write arithmetic in blob_read_range /
/// blob_write_range: local-only, overflow-only, and spans that cross the
/// local↔overflow boundary and individual overflow-page boundaries, plus the last
/// partial page, single bytes, and the whole value. SQL `substr` and a full-value
/// SQL read are the independent oracles for the byte-addressing math.
#[test]
fn blob_boundary_read_write_sweep() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB)");
    let n = 20000usize; // several overflow pages on the default page size
    exec(&conn, &format!("INSERT INTO t VALUES (1, zeroblob({n}))"));
    // Deterministic, position-dependent pattern so a misplaced byte is caught.
    let pattern: Vec<u8> = (0..n)
        .map(|i| (i.wrapping_mul(31).wrapping_add(7)) as u8)
        .collect();

    // Fill the whole value via one blob write, then verify the WRITE path with an
    // independent full-value SQL read (record decoding, not blob byte access).
    {
        let mut blob = conn.blob_open("t", "data", 1, true).unwrap();
        assert_eq!(blob.bytes(), n);
        blob.write(0, &pattern).unwrap();
        blob.close().unwrap();
    }
    let mut q = conn
        .query("SELECT data FROM t WHERE id = 1")
        .unwrap()
        .unwrap();
    let got = q.run_collect_rows().unwrap();
    assert_eq!(
        blob_bytes(&got[0][0]),
        pattern,
        "full-value SQL read after blob write"
    );
    drop(q);

    // Probe a wide grid of offsets/lengths, concentrated around page boundaries
    // (the exact bytes where the local/overflow and page-to-page seams live), and
    // verify blob reads against the known pattern AND against SQL substr.
    let bases = [
        0usize,
        1,
        2,
        3000,
        4000,
        4090,
        4091,
        4092,
        4093,
        4094,
        8180,
        8184,
        8185,
        8186,
        12276,
        16368,
        19000,
        n - 1,
        n,
    ];
    let lens = [0usize, 1, 2, 3, 100, 4091, 4092, 4093, 8184, 8185];
    let mut blob = conn.blob_open("t", "data", 1, false).unwrap();
    for &base in &bases {
        for &len in &lens {
            if base + len > n {
                continue;
            }
            let mut buf = vec![0u8; len];
            blob.read(base, &mut buf).unwrap();
            assert_eq!(
                buf,
                &pattern[base..base + len],
                "blob read mismatch at off={base} len={len}"
            );
            // Independent oracle: SQL substr addresses the same bytes.
            if len > 0 {
                let mut sq = conn
                    .query(format!(
                        "SELECT substr(data, {}, {len}) FROM t WHERE id = 1",
                        base + 1
                    ))
                    .unwrap()
                    .unwrap();
                let sr = sq.run_collect_rows().unwrap();
                assert_eq!(
                    blob_bytes(&sr[0][0]),
                    &pattern[base..base + len],
                    "substr oracle mismatch at off={base} len={len}"
                );
            }
        }
    }
    blob.close().unwrap();

    // Boundary-crossing WRITES: overwrite ranges that straddle the seams, then read
    // back through SQL substr (independent of the blob write path).
    let mut blob = conn.blob_open("t", "data", 1, true).unwrap();
    for &(off, len) in &[(4090usize, 6usize), (4092, 8184), (0, 4093), (19990, 10)] {
        let repl: Vec<u8> = (0..len).map(|i| (255 - (i as u8))).collect();
        blob.write(off, &repl).unwrap();
        let mut sq = conn
            .query(format!(
                "SELECT substr(data, {}, {len}) FROM t WHERE id = 1",
                off + 1
            ))
            .unwrap()
            .unwrap();
        let sr = sq.run_collect_rows().unwrap();
        assert_eq!(
            blob_bytes(&sr[0][0]),
            repl,
            "boundary write off={off} len={len}"
        );
    }
    blob.close().unwrap();
}
