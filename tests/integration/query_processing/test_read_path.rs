use crate::common::{limbo_exec_rows, sqlite_exec_rows, ExecRows, TempDatabase};
use rusqlite::Connection as SqliteConnection;
use tempfile::TempDir;
use turso_core::{LimboError, Numeric, StepResult, Value};

#[turso_macros::test(mvcc, init_sql = "create table test (i integer);")]
fn test_statement_reset_bind(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let mut stmt = conn.prepare("select ?")?;

    stmt.bind_at(1.try_into()?, Value::from_i64(1));
    stmt.run_with_row_callback(|row| {
        assert_eq!(
            *row.get::<&Value>(0).unwrap(),
            turso_core::Value::from_i64(1)
        );
        Ok(())
    })
    .unwrap();

    stmt.reset()?;

    stmt.bind_at(1.try_into()?, Value::from_i64(2));

    stmt.run_with_row_callback(|row| {
        assert_eq!(
            *row.get::<&Value>(0).unwrap(),
            turso_core::Value::from_i64(2)
        );
        Ok(())
    })
    .unwrap();

    Ok(())
}

#[turso_macros::test(mvcc, init_sql = "create table test (i integer);")]
fn test_statement_bind(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let mut stmt = conn.prepare("select ?, ?1, :named, ?3, ?4")?;

    stmt.bind_at(1.try_into()?, Value::build_text("hello"));

    let i = stmt.parameters().index(":named").unwrap();
    stmt.bind_at(i, Value::from_i64(42));

    stmt.bind_at(3.try_into()?, Value::from_blob(vec![0x1, 0x2, 0x3]));

    stmt.bind_at(4.try_into()?, Value::from_f64(0.5));

    assert_eq!(stmt.parameters().count(), 4);

    stmt.run_with_row_callback(|row| {
        if let turso_core::Value::Text(s) = row.get::<&Value>(0).unwrap() {
            assert_eq!(s.as_str(), "hello")
        }

        if let turso_core::Value::Text(s) = row.get::<&Value>(1).unwrap() {
            assert_eq!(s.as_str(), "hello")
        }

        if let turso_core::Value::Numeric(Numeric::Integer(i)) = row.get::<&Value>(2).unwrap() {
            assert_eq!(*i, 42)
        }

        if let turso_core::Value::Blob(v) = row.get::<&Value>(3).unwrap() {
            assert_eq!(v.as_slice(), &vec![0x1_u8, 0x2, 0x3])
        }

        if let turso_core::Value::Numeric(Numeric::Float(f)) = row.get::<&Value>(4).unwrap() {
            assert_eq!(f64::from(*f), 0.5)
        }
        Ok(())
    })
    .unwrap();

    Ok(())
}

#[turso_macros::test]
fn test_open_existing_without_rowid_database(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("without_rowid.db");
    let query = "SELECT key, value FROM config ORDER BY key";
    let schema_query = "SELECT sql FROM sqlite_schema WHERE name = 'config'";

    let (sqlite_rows, sqlite_schema) = {
        let sqlite = SqliteConnection::open(&db_path)?;
        sqlite.pragma_update(None, "journal_mode", "wal")?;
        sqlite.execute_batch(
            "CREATE TABLE config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID;
            INSERT INTO config VALUES ('language', 'en');
            INSERT INTO config VALUES ('theme', 'dark');
            PRAGMA wal_checkpoint(TRUNCATE);",
        )?;

        (
            sqlite_exec_rows(&sqlite, query),
            sqlite_exec_rows(&sqlite, schema_query),
        )
    };

    let db = TempDatabase::new_with_existent(&db_path);
    let conn = db.connect_limbo();

    assert_eq!(limbo_exec_rows(&conn, query), sqlite_rows);
    assert_eq!(limbo_exec_rows(&conn, schema_query), sqlite_schema);

    let err = conn.prepare("SELECT rowid FROM config").unwrap_err();
    assert!(
        err.to_string().contains("no such column: rowid"),
        "expected rowid access to be rejected for WITHOUT ROWID table, got {err}"
    );

    Ok(())
}

#[turso_macros::test(
    mvcc,
    init_sql = "create table test (a integer, b integer, c integer, d integer);"
)]
fn test_insert_parameter_remap(tmp_db: TempDatabase) -> anyhow::Result<()> {
    // ───────────────────────  schema  ──────────────────────────────
    // Table             a     b     c     d
    // INSERT lists:     d ,   c ,   a ,   b
    // VALUES list:    22 ,   ?1 ,   7 ,   ?2
    //
    // Expected row on disk:  a = 7 , b = ?2 , c = ?1 , d = 22
    //
    // We bind ?1 = 111 and ?2 = 222 and expect (7,222,111,22).
    // ───────────────────────────────────────────────────────────────

    let conn = tmp_db.connect_limbo();

    // prepare INSERT with re-ordered columns and constants
    let mut ins = conn.prepare("insert into test (d, c, a, b) values (22, ?, 7, ?);")?;
    let args = [Value::from_i64(111), Value::from_i64(222)];
    for (i, arg) in args.iter().enumerate() {
        let idx = i + 1;
        ins.bind_at(idx.try_into()?, arg.clone());
    }
    ins.run_with_row_callback(|_| panic!("Unexpected row"))?;

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    sel.run_with_row_callback(|row| {
        // insert_index = 3
        // A = 7
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::from_i64(7));
        // insert_index = 4
        // B = 222
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::from_i64(222));
        // insert_index = 2
        // C = 111
        assert_eq!(row.get::<&Value>(2).unwrap(), &Value::from_i64(111));
        // insert_index = 1
        // D = 22
        assert_eq!(row.get::<&Value>(3).unwrap(), &Value::from_i64(22));
        Ok(())
    })?;

    // exactly two distinct parameters were used
    assert_eq!(ins.parameters().count(), 2);

    Ok(())
}

#[turso_macros::test(
    mvcc,
    init_sql = "create table test (a integer, b integer, c integer, d integer);"
)]
fn test_insert_parameter_remap_all_params(tmp_db: TempDatabase) -> anyhow::Result<()> {
    // ───────────────────────  schema  ──────────────────────────────
    // Table             a     b     c     d
    // INSERT lists:     d ,   a ,   c ,   b
    // VALUES list:     ?1 ,  ?2 ,  ?3 ,  ?4
    //
    // Expected row on disk:  a = ?2 , b = ?4 , c = ?3 , d = ?1
    //
    // We bind ?1 = 999, ?2 = 111, ?3 = 333, ?4 = 444.
    // The row should be (111, 444, 333, 999).
    // ───────────────────────────────────────────────────────────────

    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (d, a, c, b) values (?, ?, ?, ?);")?;

    let values = [
        Value::from_i64(999), // ?1 → d
        Value::from_i64(111), // ?2 → a
        Value::from_i64(333), // ?3 → c
        Value::from_i64(444), // ?4 → b
    ];
    for (i, value) in values.iter().enumerate() {
        let idx = i + 1;
        ins.bind_at(idx.try_into()?, value.clone());
    }

    // execute the insert (no rows returned)
    ins.run_with_row_callback(|_| panic!("Unexpected row"))?;

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    sel.run_with_row_callback(|row| {
        // insert_index = 2
        // A = 111
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::from_i64(111));
        // insert_index = 4
        // B = 444
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::from_i64(444));
        // insert_index = 3
        // C = 333
        assert_eq!(row.get::<&Value>(2).unwrap(), &Value::from_i64(333));
        // insert_index = 1
        // D = 999
        assert_eq!(row.get::<&Value>(3).unwrap(), &Value::from_i64(999));
        Ok(())
    })?;

    assert_eq!(ins.parameters().count(), 4);
    Ok(())
}

#[turso_macros::test(
    mvcc,
    init_sql = "create table test (a integer, b integer, c integer, d integer);"
)]
fn test_insert_parameter_multiple_remap_backwards(tmp_db: TempDatabase) -> anyhow::Result<()> {
    // ───────────────────────  schema  ──────────────────────────────
    // Table             a     b     c     d
    // INSERT lists:     d ,   c ,   b ,   a
    // VALUES list:     ?1 ,  ?2 ,  ?3 ,  ?4
    //
    // Expected row on disk:  a = ?1 , b = ?2 , c = ?3 , d = ?4
    //
    // The row should be (111, 222, 333, 444)
    // ───────────────────────────────────────────────────────────────

    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (d,c,b,a) values (?, ?, ?, ?);")?;

    let values = [
        Value::from_i64(444), // ?1 → d
        Value::from_i64(333), // ?2 → c
        Value::from_i64(222), // ?3 → b
        Value::from_i64(111), // ?4 → a
    ];
    for (i, value) in values.iter().enumerate() {
        let idx = i + 1;
        ins.bind_at(idx.try_into()?, value.clone());
    }

    // execute the insert (no rows returned)
    ins.run_with_row_callback(|_| panic!("Unexpected row"))?;

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    sel.run_with_row_callback(|row| {
        // insert_index = 2
        // A = 111
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::from_i64(111));
        // insert_index = 4
        // B = 444
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::from_i64(222));
        // insert_index = 3
        // C = 333
        assert_eq!(row.get::<&Value>(2).unwrap(), &Value::from_i64(333));
        // insert_index = 1
        // D = 999
        assert_eq!(row.get::<&Value>(3).unwrap(), &Value::from_i64(444));
        Ok(())
    })?;

    assert_eq!(ins.parameters().count(), 4);
    Ok(())
}

#[turso_macros::test(
    mvcc,
    init_sql = "create table test (a integer, b integer, c integer, d integer);"
)]
fn test_insert_parameter_multiple_no_remap(tmp_db: TempDatabase) -> anyhow::Result<()> {
    // ───────────────────────  schema  ──────────────────────────────
    // Table             a     b     c     d
    // INSERT lists:     a ,   b ,   c ,   d
    // VALUES list:     ?1 ,  ?2 ,  ?3 ,  ?4
    //
    // Expected row on disk:  a = ?1 , b = ?2 , c = ?3 , d = ?4
    //
    // The row should be (111, 222, 333, 444)
    // ───────────────────────────────────────────────────────────────

    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (a,b,c,d) values (?, ?, ?, ?);")?;

    let values = [
        Value::from_i64(111), // ?1 → a
        Value::from_i64(222), // ?2 → b
        Value::from_i64(333), // ?3 → c
        Value::from_i64(444), // ?4 → d
    ];
    for (i, value) in values.iter().enumerate() {
        let idx = i + 1;
        ins.bind_at(idx.try_into()?, value.clone());
    }

    // execute the insert (no rows returned)
    ins.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    sel.run_with_row_callback(|row| {
        // insert_index = 2
        // A = 111
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::from_i64(111));
        // insert_index = 4
        // B = 444
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::from_i64(222));
        // insert_index = 3
        // C = 333
        assert_eq!(row.get::<&Value>(2).unwrap(), &Value::from_i64(333));
        // insert_index = 1
        // D = 999
        assert_eq!(row.get::<&Value>(3).unwrap(), &Value::from_i64(444));
        Ok(())
    })?;
    assert_eq!(ins.parameters().count(), 4);
    Ok(())
}

#[turso_macros::test(
    mvcc,
    init_sql = "create table test (a integer, b integer, c integer, d integer);"
)]
fn test_insert_parameter_multiple_row(tmp_db: TempDatabase) -> anyhow::Result<()> {
    // ───────────────────────  schema  ──────────────────────────────
    // Table             a     b     c     d
    // INSERT lists:     b ,   a ,   d ,   c
    // VALUES list:     (?1 ,  ?2 ,  ?3 ,  ?4),
    //                  (?5,   ?6,   ?7,   ?8);
    //
    // The row should be (111, 222, 333, 444), (555, 666, 777, 888)
    // ───────────────────────────────────────────────────────────────

    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (b,a,d,c) values (?, ?, ?, ?), (?, ?, ?, ?);")?;

    let values = [
        Value::from_i64(222), // ?1 → b
        Value::from_i64(111), // ?2 → a
        Value::from_i64(444), // ?3 → d
        Value::from_i64(333), // ?4 → c
        Value::from_i64(666), // ?1 → b
        Value::from_i64(555), // ?2 → a
        Value::from_i64(888), // ?3 → d
        Value::from_i64(777), // ?4 → c
    ];
    for (i, value) in values.iter().enumerate() {
        let idx = i + 1;
        ins.bind_at(idx.try_into()?, value.clone());
    }

    // execute the insert (no rows returned)
    ins.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    let mut i = 0;
    sel.run_with_row_callback(|row| {
        assert_eq!(
            row.get::<&Value>(0).unwrap(),
            &Value::from_i64(if i == 0 { 111 } else { 555 })
        );
        assert_eq!(
            row.get::<&Value>(1).unwrap(),
            &Value::from_i64(if i == 0 { 222 } else { 666 })
        );
        assert_eq!(
            row.get::<&Value>(2).unwrap(),
            &Value::from_i64(if i == 0 { 333 } else { 777 })
        );
        assert_eq!(
            row.get::<&Value>(3).unwrap(),
            &Value::from_i64(if i == 0 { 444 } else { 888 })
        );
        i += 1;
        Ok(())
    })?;
    assert_eq!(ins.parameters().count(), 8);
    Ok(())
}

#[turso_macros::test(mvcc, init_sql = "create table test (a integer, b text);")]
fn test_bind_parameters_update_query(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (a, b) values (3, 'test1');")?;
    ins.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut ins = conn.prepare("update test set a = ? where b = ?;")?;
    ins.bind_at(1.try_into()?, Value::from_i64(222));
    ins.bind_at(2.try_into()?, Value::build_text("test1"));

    ins.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut sel = conn.prepare("select a, b from test;")?;
    sel.run_with_row_callback(|row| {
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::from_i64(222));
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::build_text("test1"),);
        Ok(())
    })?;
    assert_eq!(ins.parameters().count(), 2);
    Ok(())
}

#[turso_macros::test(
    mvcc,
    init_sql = "create table test (a integer, b text, c integer, d integer);"
)]
fn test_bind_parameters_update_query_multiple_where(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (a, b, c, d) values (3, 'test1', 4, 5);")?;
    ins.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut ins = conn.prepare("update test set a = ? where b = ? and c = 4 and d = ?;")?;
    ins.bind_at(1.try_into()?, Value::from_i64(222));
    ins.bind_at(2.try_into()?, Value::build_text("test1"));
    ins.bind_at(3.try_into()?, Value::from_i64(5));
    ins.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    sel.run_with_row_callback(|row| {
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::from_i64(222));
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::build_text("test1"),);
        assert_eq!(row.get::<&Value>(2).unwrap(), &Value::from_i64(4));
        assert_eq!(row.get::<&Value>(3).unwrap(), &Value::from_i64(5));
        Ok(())
    })?;
    assert_eq!(ins.parameters().count(), 3);
    Ok(())
}

#[turso_macros::test(
    mvcc,
    init_sql = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);"
)]
fn test_bind_parameters_update_rowid_alias(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (id, name) values (1, 'test');")?;
    ins.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut sel = conn.prepare("select id, name from test;")?;
    sel.run_with_row_callback(|row| {
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::from_i64(1));
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::build_text("test"),);
        Ok(())
    })?;

    let mut ins = conn.prepare("update test set name = ? where id = ?;")?;
    ins.bind_at(1.try_into()?, Value::build_text("updated"));
    ins.bind_at(2.try_into()?, Value::from_i64(1));
    ins.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut sel = conn.prepare("select id, name from test;")?;
    sel.run_with_row_callback(|row| {
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::from_i64(1));
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::build_text("updated"),);
        Ok(())
    })?;
    assert_eq!(ins.parameters().count(), 2);
    Ok(())
}

#[turso_macros::test(
    mvcc,
    init_sql = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age integer);"
)]
fn test_bind_parameters_update_rowid_alias_seek_rowid(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("insert into test (id, name, age) values (1, 'test', 4);")?;
    conn.execute("insert into test (id, name, age) values (2, 'test', 11);")?;

    let mut sel = conn.prepare("select id, name, age from test;")?;
    let mut i = 0;
    sel.run_with_row_callback(|row| {
        assert_eq!(
            row.get::<&Value>(0).unwrap(),
            &Value::from_i64(if i == 0 { 1 } else { 2 })
        );
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::build_text("test"),);
        assert_eq!(
            row.get::<&Value>(2).unwrap(),
            &Value::from_i64(if i == 0 { 4 } else { 11 })
        );
        i += 1;
        Ok(())
    })?;

    let mut ins = conn.prepare("update test set name = ? where id < ? AND age between ? and ?;")?;
    ins.bind_at(1.try_into()?, Value::build_text("updated"));
    ins.bind_at(2.try_into()?, Value::from_i64(2));
    ins.bind_at(3.try_into()?, Value::from_i64(3));
    ins.bind_at(4.try_into()?, Value::from_i64(5));
    ins.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut sel = conn.prepare("select name from test;")?;
    let mut i = 0;
    sel.run_with_row_callback(|row| {
        assert_eq!(
            row.get::<&Value>(0).unwrap(),
            &Value::build_text(if i == 0 { "updated" } else { "test" }),
        );
        i += 1;
        Ok(())
    })?;

    assert_eq!(ins.parameters().count(), 4);
    Ok(())
}

// TODO: mvcc fails with `BTree should have returned rowid after next`
#[turso_macros::test(
    init_sql = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age integer);"
)]
fn test_bind_parameters_delete_rowid_alias_seek_out_of_order(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("insert into test (id, name, age) values (1, 'correct', 4);")?;
    conn.execute("insert into test (id, name, age) values (5, 'test', 11);")?;

    let mut ins =
        conn.prepare("delete from test where age between ? and ? AND id > ? AND name = ?;")?;
    ins.bind_at(1.try_into()?, Value::from_i64(10));
    ins.bind_at(2.try_into()?, Value::from_i64(12));
    ins.bind_at(3.try_into()?, Value::from_i64(4));
    ins.bind_at(4.try_into()?, Value::build_text("test"));
    ins.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut sel = conn.prepare("select name from test;")?;
    let mut i = 0;
    sel.run_with_row_callback(|row| {
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::build_text("correct"),);
        i += 1;
        Ok(())
    })?;

    assert_eq!(i, 1);
    assert_eq!(ins.parameters().count(), 4);
    Ok(())
}

#[turso_macros::test(
    mvcc,
    init_sql = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);"
)]
fn test_cte_alias(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO test (id, name) VALUES (1, 'Limbo');")?;
    conn.execute("INSERT INTO test (id, name) VALUES (2, 'Turso');")?;

    let mut stmt1 = conn.prepare(
        "WITH a1 AS (SELECT id FROM test WHERE name = 'Limbo') SELECT a2.id FROM a1 AS a2",
    )?;
    stmt1.run_with_row_callback(|row| {
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::from_i64(1));
        Ok(())
    })?;

    let mut stmt2 = conn
        .prepare("WITH a1 AS (SELECT id FROM test WHERE name = 'Turso') SELECT a2.id FROM a1 a2")?;
    stmt2.run_with_row_callback(|row| {
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::from_i64(2));
        Ok(())
    })?;
    Ok(())
}

#[turso_macros::test(
    mvcc,
    init_sql = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);"
)]
fn test_cte_with_union(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")?;
    conn.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")?;

    // Test 1: CTE with UNION ALL - CTE used in first SELECT
    let mut stmt = conn.prepare(
        "WITH t AS (SELECT id, name FROM test WHERE id = 1) SELECT * FROM t UNION ALL SELECT 99, 'Extra'",
    )?;
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect::<Vec<_>>());
        Ok(())
    })?;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::from_i64(1));
    assert_eq!(rows[1][0], Value::from_i64(99));

    // Test 2: CTE with UNION (not UNION ALL)
    let mut stmt = conn.prepare("WITH t AS (SELECT 1 as x) SELECT * FROM t UNION SELECT 2 as x")?;
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect::<Vec<_>>());
        Ok(())
    })?;

    assert_eq!(rows.len(), 2);

    // Test 3: Multiple CTEs with UNION ALL - both CTEs used in different branches
    let mut stmt = conn.prepare(
        "WITH t1 AS (SELECT id FROM test WHERE id = 1), t2 AS (SELECT id FROM test WHERE id = 2) \
         SELECT * FROM t1 UNION ALL SELECT * FROM t2",
    )?;
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect::<Vec<_>>());
        Ok(())
    })?;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::from_i64(1));
    assert_eq!(rows[1][0], Value::from_i64(2));

    Ok(())
}

#[turso_macros::test(mvcc, init_sql = "create table t (x, y);")]
fn test_avg_agg(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("insert into t values (1, null), (2, null), (3, null), (null, null), (4, null)")?;
    let mut rows = Vec::new();
    let mut stmt = conn.prepare("select avg(x), avg(y) from t")?;
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect::<Vec<_>>());
        Ok(())
    })?;

    assert_eq!(stmt.num_columns(), 2);
    assert_eq!(stmt.get_column_name(0), "avg(x)");
    assert_eq!(stmt.get_column_name(1), "avg(y)");

    assert_eq!(
        rows,
        vec![vec![
            turso_core::Value::from_f64((1.0 + 2.0 + 3.0 + 4.0) / (4.0)),
            turso_core::Value::Null
        ]]
    );

    Ok(())
}

#[turso_macros::test(mvcc, init_sql = "CREATE TABLE test (i INTEGER);")]
fn test_offset_limit_bind(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("INSERT INTO test VALUES (5), (4), (3), (2), (1)")?;

    for (limit, offset, expected) in [
        (
            2,
            1,
            vec![
                vec![turso_core::Value::from_i64(4)],
                vec![turso_core::Value::from_i64(3)],
            ],
        ),
        (0, 0, vec![]),
        (1, 0, vec![vec![turso_core::Value::from_i64(5)]]),
        (0, 1, vec![]),
        (1, 1, vec![vec![turso_core::Value::from_i64(4)]]),
    ] {
        let mut stmt = conn.prepare("SELECT * FROM test LIMIT ? OFFSET ?")?;
        stmt.bind_at(1.try_into()?, Value::from_i64(limit));
        stmt.bind_at(2.try_into()?, Value::from_i64(offset));

        let mut rows = Vec::new();
        stmt.run_with_row_callback(|row| {
            rows.push(row.get_values().cloned().collect::<Vec<_>>());
            Ok(())
        })?;

        assert_eq!(rows, expected);
    }

    Ok(())
}

#[turso_macros::test(
    mvcc,
    init_sql = "CREATE TABLE test (k INTEGER PRIMARY KEY, v INTEGER);"
)]
fn test_upsert_parameters_order(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("INSERT INTO test VALUES (1, 2), (3, 4)")?;
    let mut stmt =
        conn.prepare("INSERT INTO test VALUES (?, ?), (?, ?) ON CONFLICT DO UPDATE SET v = ?")?;
    stmt.bind_at(1.try_into()?, Value::from_i64(1));
    stmt.bind_at(2.try_into()?, Value::from_i64(20));
    stmt.bind_at(3.try_into()?, Value::from_i64(3));
    stmt.bind_at(4.try_into()?, Value::from_i64(40));
    stmt.bind_at(5.try_into()?, Value::from_i64(66));
    stmt.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut rows = Vec::new();
    let mut stmt = conn.prepare("SELECT * FROM test")?;
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect::<Vec<_>>());
        Ok(())
    })?;

    assert_eq!(
        rows,
        vec![
            vec![
                turso_core::Value::from_i64(1),
                turso_core::Value::from_i64(66)
            ],
            vec![
                turso_core::Value::from_i64(3),
                turso_core::Value::from_i64(66)
            ]
        ]
    );
    Ok(())
}

// TODO: mvcc fails with:
// tests/integration/query_processing/test_read_path.rs:883:5:
// assertion `left == right` failed
//   left: [[Integer(0)]]
//  right: [[Integer(2)]]
#[turso_macros::test(init_sql = "CREATE TABLE test (k INTEGER PRIMARY KEY, v INTEGER);")]
fn test_multiple_connections_visibility(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();
    conn1.execute("BEGIN")?;
    conn1.execute("INSERT INTO test VALUES (1, 2), (3, 4)")?;
    let mut stmt = conn2.prepare("SELECT COUNT(*) FROM test").unwrap();
    let _ = stmt.step().unwrap();
    // intentionally drop not-fully-consumed statement in order to check that on Drop statement will execute reset with proper cleanup
    drop(stmt);
    conn1.execute("COMMIT")?;

    let rows: Vec<(i64,)> = conn2.exec_rows("SELECT COUNT(*) FROM test");
    assert_eq!(rows, vec![(2,)]);
    Ok(())
}

#[turso_macros::test(mvcc, init_sql = "CREATE TABLE test (x);")]
fn test_stmt_reset(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn1 = tmp_db.connect_limbo();
    let mut stmt1 = conn1.prepare("INSERT INTO test VALUES (?)").unwrap();
    for _ in 0..3 {
        stmt1.reset()?;
        stmt1.bind_at(1.try_into().unwrap(), Value::Blob(vec![0u8; 1024]));
        loop {
            match stmt1.step().unwrap() {
                StepResult::Done => break,
                _ => tmp_db.io.step().unwrap(),
            }
        }
    }

    // force btree-page split which will be "unnoticed" by stmt1 if it will cache something in between of calls
    conn1
        .execute("INSERT INTO test VALUES (randomblob(1024))")
        .unwrap();

    stmt1.reset()?;
    stmt1.bind_at(1.try_into().unwrap(), Value::Blob(vec![0u8; 1024]));
    loop {
        match stmt1.step().unwrap() {
            StepResult::Done => break,
            _ => tmp_db.io.step().unwrap(),
        }
    }
    let rows: Vec<(i64,)> = conn1.exec_rows("SELECT rowid FROM test");
    assert_eq!(rows, vec![(1,), (2,), (3,), (4,), (5,)]);
    Ok(())
}

#[turso_macros::test]
fn test_prepare_rejects_empty_statements(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    let empty_inputs = [
        ";",
        ";;;",
        "   ",
        "\n\t",
        "-- comment",
        "/* comment */",
        "/**/",
    ];

    for sql in empty_inputs {
        let Err(err) = conn.prepare(sql) else {
            panic!("Expected invalid argument error for input: {sql}");
        };
        match err {
            LimboError::InvalidArgument(msg) => {
                assert!(
                    msg.contains("contains no statements"),
                    "Unexpected error message for input {sql}: {msg}"
                );
            }
            other => panic!("Unexpected error for input {sql}: {other}"),
        }
    }

    let invalid_syntax_inputs = ["/* outer /* inner */ still outer */"];
    for sql in invalid_syntax_inputs {
        let Err(err) = conn.prepare(sql) else {
            panic!("Expected parse error for input: {sql}");
        };
        match err {
            LimboError::ParseError(_) | LimboError::LexerError(_) => {}
            other => panic!("Unexpected error for input {sql}: {other}"),
        }
    }
}

#[turso_macros::test]
/// Test that we can only join up to 63 tables, and trying to join more should fail with an error instead of panicing.
fn test_max_joined_tables_limit(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    // Create 64 tables
    for i in 0..64 {
        conn.execute(format!("CREATE TABLE t{i} (id INTEGER)"))
            .unwrap();
    }

    // Try to join 64 tables - should fail
    let mut sql = String::from("SELECT * FROM t0");
    for i in 1..64 {
        sql.push_str(&format!(" JOIN t{i} ON t{i}.id = t0.id"));
    }

    let Err(LimboError::ParseError(result)) = conn.prepare(&sql) else {
        panic!("Expected an error but got no error");
    };
    assert!(result.contains("Only up to 63 tables can be joined"));
}

#[turso_macros::test]
/// Test that we can create and select from a table with 1000 columns.
fn test_many_columns(tmp_db: TempDatabase) {
    let mut create_sql = String::from("CREATE TABLE test (");
    for i in 0..1000 {
        if i > 0 {
            create_sql.push_str(", ");
        }
        create_sql.push_str(&format!("col{i} INTEGER"));
    }
    create_sql.push(')');

    let conn = tmp_db.connect_limbo();
    conn.execute(&create_sql).unwrap();

    // Insert a row with values 0-999
    let mut insert_sql = String::from("INSERT INTO test VALUES (");
    for i in 0..1000 {
        if i > 0 {
            insert_sql.push_str(", ");
        }
        insert_sql.push_str(&i.to_string());
    }
    insert_sql.push(')');
    conn.execute(&insert_sql).unwrap();

    // Select every 100th column
    let mut select_sql = String::from("SELECT ");
    let mut first = true;
    for i in (0..1000).step_by(100) {
        if !first {
            select_sql.push_str(", ");
        }
        select_sql.push_str(&format!("col{i}"));
        first = false;
    }
    select_sql.push_str(" FROM test");

    let mut rows = Vec::new();
    let mut stmt = conn.prepare(&select_sql).unwrap();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect::<Vec<_>>());
        Ok(())
    })
    .unwrap();

    // Verify we got values 0,100,200,...,900
    assert_eq!(
        rows,
        vec![vec![
            turso_core::Value::from_i64(0),
            turso_core::Value::from_i64(100),
            turso_core::Value::from_i64(200),
            turso_core::Value::from_i64(300),
            turso_core::Value::from_i64(400),
            turso_core::Value::from_i64(500),
            turso_core::Value::from_i64(600),
            turso_core::Value::from_i64(700),
            turso_core::Value::from_i64(800),
            turso_core::Value::from_i64(900),
        ]]
    );
}

#[turso_macros::test]
fn test_eval_param_only_once(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t SELECT value FROM generate_series(1, 10000)")
        .unwrap();
    let mut stmt = conn
        .query("SELECT COUNT(*) FROM t WHERE LENGTH(zeroblob(?)) = ?")
        .unwrap()
        .unwrap();
    stmt.bind_at(
        1.try_into().unwrap(),
        turso_core::Value::from_i64(100_000_000),
    );
    stmt.bind_at(
        2.try_into().unwrap(),
        turso_core::Value::from_i64(100_000_000),
    );
    let start_time = std::time::Instant::now();
    stmt.run_with_row_callback(|row| {
        let values = row.get_values().cloned().collect::<Vec<_>>();
        assert_eq!(values, vec![turso_core::Value::from_i64(10000)]);
        Ok(())
    })
    .unwrap();

    let end_time = std::time::Instant::now();
    let elapsed = end_time.duration_since(start_time);
    // the test will allocate 10^8 * 10^4 bytes in case if parameter will be evaluated for every row
    assert!(elapsed < std::time::Duration::from_millis(500));
}

/// Regression test for https://github.com/tursodatabase/turso/issues/5232
/// SELECT with more than SQLITE_MAX_COLUMN (2000) columns should return an error,
/// not panic from u16 overflow.
#[turso_macros::test]
fn test_too_many_columns_in_select(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    // 2001 columns should exceed the SQLITE_MAX_COLUMN limit of 2000
    let columns = std::iter::repeat_n("1", 2001).collect::<Vec<_>>().join(",");
    let query = format!("SELECT {columns}");
    let result = conn.prepare(&query);
    assert!(
        result.is_err(),
        "Expected error for SELECT with 2001 columns"
    );
    let err = result.unwrap_err();
    assert!(
        matches!(err, LimboError::ParseError(ref msg) if msg.contains("too many columns")),
        "Expected 'too many columns' error, got: {err}"
    );

    // 2000 columns should be fine
    let columns = std::iter::repeat_n("1", 2000).collect::<Vec<_>>().join(",");
    let query = format!("SELECT {columns}");
    let result = conn.prepare(&query);
    assert!(result.is_ok(), "SELECT with 2000 columns should succeed");

    // UNION with too many columns should also error
    let columns = std::iter::repeat_n("1", 2001).collect::<Vec<_>>().join(",");
    let query = format!("SELECT {columns} UNION SELECT {columns}");
    let result = conn.prepare(&query);
    assert!(
        result.is_err(),
        "Expected error for UNION with 2001 columns"
    );
}

#[turso_macros::test(
    init_sql = "CREATE TABLE profiles (id INTEGER PRIMARY KEY, bio TEXT, user_id INTEGER NOT NULL)"
)]
fn test_bind_in_exists_subquery(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let query = "SELECT id, bio, user_id FROM profiles WHERE EXISTS (SELECT ?2 AS column1 FROM (VALUES (?1)) AS tbl_1_0 WHERE tbl_1_0.column1 = profiles.user_id)";

    // Verify expected behavior against sqlite (rusqlite) first.
    // init_sql already created the table via rusqlite; insert test data and
    // checkpoint so the row is visible to both engines.
    {
        let sqlite_conn = rusqlite::Connection::open(&tmp_db.path)?;
        sqlite_conn.execute(
            "INSERT INTO profiles (bio, user_id) VALUES ('Hello', 42)",
            [],
        )?;
        sqlite_conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;

        let mut stmt = sqlite_conn.prepare(query)?;
        let sqlite_rows: Vec<(i64, String, i64)> = stmt
            .query_map(rusqlite::params![42, 1], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(sqlite_rows.len(), 1);
        assert_eq!(sqlite_rows[0].2, 42);
    }

    // Now verify turso matches sqlite with the same bound parameters.
    // Use has_slot() assertions to validate parameter registration — this is
    // the same check that bind_positional() performs in the Rust binding.
    // bind_at() alone silently succeeds even when parameters are unregistered,
    // which masked the bug this test is meant to catch.
    let conn = tmp_db.connect_limbo();
    let mut stmt = conn.prepare(query)?;
    assert!(
        stmt.parameters().has_slot(1.try_into().unwrap()),
        "parameter ?1 should be registered"
    );
    assert!(
        stmt.parameters().has_slot(2.try_into().unwrap()),
        "parameter ?2 should be registered"
    );
    stmt.bind_at(1.try_into()?, Value::from_i64(42));
    stmt.bind_at(2.try_into()?, Value::from_i64(1));
    let mut turso_rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        turso_rows.push(row.get::<&Value>(2).unwrap().clone());
        Ok(())
    })?;
    assert_eq!(turso_rows.len(), 1);
    assert_eq!(turso_rows[0], Value::from_i64(42));
    Ok(())
}

/// Column names for bound parameters must match SQLite:
///   bare `?` → "?", explicit `?NNN` → "?NNN", named → verbatim text.
#[turso_macros::test(mvcc)]
fn test_parameter_column_names(tmp_db: TempDatabase) {
    let cases: &[(&str, &[&str])] = &[
        ("SELECT ?, ?", &["?", "?"]),
        ("SELECT ?1, ?2", &["?1", "?2"]),
        ("SELECT ?999", &["?999"]),
        ("SELECT :foo", &[":foo"]),
        ("SELECT @bar", &["@bar"]),
        ("SELECT $baz", &["$baz"]),
        (
            "SELECT ?, ?1, :foo, @bar, $baz",
            &["?", "?1", ":foo", "@bar", "$baz"],
        ),
        ("SELECT ? AS alias", &["alias"]),
    ];

    // Verify expected values against rusqlite (bundled SQLite)
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for (sql, expected) in cases {
        let stmt = sqlite_conn.prepare(sql).unwrap();
        let names: Vec<&str> = stmt.column_names();
        assert_eq!(names, *expected, "SQLite column names mismatch for: {sql}");
    }

    // Verify Turso matches
    let conn = tmp_db.connect_limbo();
    for (sql, expected) in cases {
        let stmt = conn.prepare(sql).unwrap();
        let names: Vec<String> = (0..stmt.num_columns())
            .map(|i| stmt.get_column_name(i).to_string())
            .collect();
        let expected: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
        assert_eq!(names, expected, "Turso column names mismatch for: {sql}");
    }
}
