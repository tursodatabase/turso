use crate::common::{ExecRows, TempDatabase};
use turso_core::{LimboError, StepResult, Value};

#[turso_macros::test(mvcc, init_sql = "create table test (i integer);")]
fn test_statement_reset_bind(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let mut stmt = conn.prepare("select ?")?;

    stmt.bind_at(1.try_into()?, Value::Integer(1));
    stmt.run_with_row_callback(|row| {
        assert_eq!(
            *row.get::<&Value>(0).unwrap(),
            turso_core::Value::Integer(1)
        );
        Ok(())
    })
    .unwrap();

    stmt.reset();

    stmt.bind_at(1.try_into()?, Value::Integer(2));

    stmt.run_with_row_callback(|row| {
        assert_eq!(
            *row.get::<&Value>(0).unwrap(),
            turso_core::Value::Integer(2)
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
    stmt.bind_at(i, Value::Integer(42));

    stmt.bind_at(3.try_into()?, Value::from_blob(vec![0x1, 0x2, 0x3]));

    stmt.bind_at(4.try_into()?, Value::Float(0.5));

    assert_eq!(stmt.parameters().count(), 4);

    stmt.run_with_row_callback(|row| {
        if let turso_core::Value::Text(s) = row.get::<&Value>(0).unwrap() {
            assert_eq!(s.as_str(), "hello")
        }

        if let turso_core::Value::Text(s) = row.get::<&Value>(1).unwrap() {
            assert_eq!(s.as_str(), "hello")
        }

        if let turso_core::Value::Integer(i) = row.get::<&Value>(2).unwrap() {
            assert_eq!(*i, 42)
        }

        if let turso_core::Value::Blob(v) = row.get::<&Value>(3).unwrap() {
            assert_eq!(v.as_slice(), &vec![0x1_u8, 0x2, 0x3])
        }

        if let turso_core::Value::Float(f) = row.get::<&Value>(4).unwrap() {
            assert_eq!(*f, 0.5)
        }
        Ok(())
    })
    .unwrap();

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
    let args = [Value::Integer(111), Value::Integer(222)];
    for (i, arg) in args.iter().enumerate() {
        let idx = i + 1;
        ins.bind_at(idx.try_into()?, arg.clone());
    }
    ins.run_with_row_callback(|_| panic!("Unexpected row"))?;

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    sel.run_with_row_callback(|row| {
        // insert_index = 3
        // A = 7
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(7));
        // insert_index = 4
        // B = 222
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::Integer(222));
        // insert_index = 2
        // C = 111
        assert_eq!(row.get::<&Value>(2).unwrap(), &Value::Integer(111));
        // insert_index = 1
        // D = 22
        assert_eq!(row.get::<&Value>(3).unwrap(), &Value::Integer(22));
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
        Value::Integer(999), // ?1 → d
        Value::Integer(111), // ?2 → a
        Value::Integer(333), // ?3 → c
        Value::Integer(444), // ?4 → b
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
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(111));
        // insert_index = 4
        // B = 444
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::Integer(444));
        // insert_index = 3
        // C = 333
        assert_eq!(row.get::<&Value>(2).unwrap(), &Value::Integer(333));
        // insert_index = 1
        // D = 999
        assert_eq!(row.get::<&Value>(3).unwrap(), &Value::Integer(999));
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
        Value::Integer(444), // ?1 → d
        Value::Integer(333), // ?2 → c
        Value::Integer(222), // ?3 → b
        Value::Integer(111), // ?4 → a
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
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(111));
        // insert_index = 4
        // B = 444
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::Integer(222));
        // insert_index = 3
        // C = 333
        assert_eq!(row.get::<&Value>(2).unwrap(), &Value::Integer(333));
        // insert_index = 1
        // D = 999
        assert_eq!(row.get::<&Value>(3).unwrap(), &Value::Integer(444));
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
        Value::Integer(111), // ?1 → a
        Value::Integer(222), // ?2 → b
        Value::Integer(333), // ?3 → c
        Value::Integer(444), // ?4 → d
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
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(111));
        // insert_index = 4
        // B = 444
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::Integer(222));
        // insert_index = 3
        // C = 333
        assert_eq!(row.get::<&Value>(2).unwrap(), &Value::Integer(333));
        // insert_index = 1
        // D = 999
        assert_eq!(row.get::<&Value>(3).unwrap(), &Value::Integer(444));
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
        Value::Integer(222), // ?1 → b
        Value::Integer(111), // ?2 → a
        Value::Integer(444), // ?3 → d
        Value::Integer(333), // ?4 → c
        Value::Integer(666), // ?1 → b
        Value::Integer(555), // ?2 → a
        Value::Integer(888), // ?3 → d
        Value::Integer(777), // ?4 → c
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
            &Value::Integer(if i == 0 { 111 } else { 555 })
        );
        assert_eq!(
            row.get::<&Value>(1).unwrap(),
            &Value::Integer(if i == 0 { 222 } else { 666 })
        );
        assert_eq!(
            row.get::<&Value>(2).unwrap(),
            &Value::Integer(if i == 0 { 333 } else { 777 })
        );
        assert_eq!(
            row.get::<&Value>(3).unwrap(),
            &Value::Integer(if i == 0 { 444 } else { 888 })
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
    ins.bind_at(1.try_into()?, Value::Integer(222));
    ins.bind_at(2.try_into()?, Value::build_text("test1"));

    ins.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut sel = conn.prepare("select a, b from test;")?;
    sel.run_with_row_callback(|row| {
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(222));
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
    ins.bind_at(1.try_into()?, Value::Integer(222));
    ins.bind_at(2.try_into()?, Value::build_text("test1"));
    ins.bind_at(3.try_into()?, Value::Integer(5));
    ins.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    sel.run_with_row_callback(|row| {
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(222));
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::build_text("test1"),);
        assert_eq!(row.get::<&Value>(2).unwrap(), &Value::Integer(4));
        assert_eq!(row.get::<&Value>(3).unwrap(), &Value::Integer(5));
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
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(1));
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::build_text("test"),);
        Ok(())
    })?;

    let mut ins = conn.prepare("update test set name = ? where id = ?;")?;
    ins.bind_at(1.try_into()?, Value::build_text("updated"));
    ins.bind_at(2.try_into()?, Value::Integer(1));
    ins.run_with_row_callback(|_| panic!("unexpected row"))?;

    let mut sel = conn.prepare("select id, name from test;")?;
    sel.run_with_row_callback(|row| {
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(1));
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
            &Value::Integer(if i == 0 { 1 } else { 2 })
        );
        assert_eq!(row.get::<&Value>(1).unwrap(), &Value::build_text("test"),);
        assert_eq!(
            row.get::<&Value>(2).unwrap(),
            &Value::Integer(if i == 0 { 4 } else { 11 })
        );
        i += 1;
        Ok(())
    })?;

    let mut ins = conn.prepare("update test set name = ? where id < ? AND age between ? and ?;")?;
    ins.bind_at(1.try_into()?, Value::build_text("updated"));
    ins.bind_at(2.try_into()?, Value::Integer(2));
    ins.bind_at(3.try_into()?, Value::Integer(3));
    ins.bind_at(4.try_into()?, Value::Integer(5));
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
    ins.bind_at(1.try_into()?, Value::Integer(10));
    ins.bind_at(2.try_into()?, Value::Integer(12));
    ins.bind_at(3.try_into()?, Value::Integer(4));
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
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(1));
        Ok(())
    })?;

    let mut stmt2 = conn
        .prepare("WITH a1 AS (SELECT id FROM test WHERE name = 'Turso') SELECT a2.id FROM a1 a2")?;
    stmt2.run_with_row_callback(|row| {
        assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(2));
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
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[1][0], Value::Integer(99));

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
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[1][0], Value::Integer(2));

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
    assert_eq!(stmt.get_column_name(0), "avg (t.x)");
    assert_eq!(stmt.get_column_name(1), "avg (t.y)");

    assert_eq!(
        rows,
        vec![vec![
            turso_core::Value::Float((1.0 + 2.0 + 3.0 + 4.0) / (4.0)),
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
                vec![turso_core::Value::Integer(4)],
                vec![turso_core::Value::Integer(3)],
            ],
        ),
        (0, 0, vec![]),
        (1, 0, vec![vec![turso_core::Value::Integer(5)]]),
        (0, 1, vec![]),
        (1, 1, vec![vec![turso_core::Value::Integer(4)]]),
    ] {
        let mut stmt = conn.prepare("SELECT * FROM test LIMIT ? OFFSET ?")?;
        stmt.bind_at(1.try_into()?, Value::Integer(limit));
        stmt.bind_at(2.try_into()?, Value::Integer(offset));

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
    stmt.bind_at(1.try_into()?, Value::Integer(1));
    stmt.bind_at(2.try_into()?, Value::Integer(20));
    stmt.bind_at(3.try_into()?, Value::Integer(3));
    stmt.bind_at(4.try_into()?, Value::Integer(40));
    stmt.bind_at(5.try_into()?, Value::Integer(66));
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
                turso_core::Value::Integer(1),
                turso_core::Value::Integer(66)
            ],
            vec![
                turso_core::Value::Integer(3),
                turso_core::Value::Integer(66)
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
        stmt1.reset();
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

    stmt1.reset();
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
            turso_core::Value::Integer(0),
            turso_core::Value::Integer(100),
            turso_core::Value::Integer(200),
            turso_core::Value::Integer(300),
            turso_core::Value::Integer(400),
            turso_core::Value::Integer(500),
            turso_core::Value::Integer(600),
            turso_core::Value::Integer(700),
            turso_core::Value::Integer(800),
            turso_core::Value::Integer(900),
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
        turso_core::Value::Integer(100_000_000),
    );
    stmt.bind_at(
        2.try_into().unwrap(),
        turso_core::Value::Integer(100_000_000),
    );
    let start_time = std::time::Instant::now();
    stmt.run_with_row_callback(|row| {
        let values = row.get_values().cloned().collect::<Vec<_>>();
        assert_eq!(values, vec![turso_core::Value::Integer(10000)]);
        Ok(())
    })
    .unwrap();

    let end_time = std::time::Instant::now();
    let elapsed = end_time.duration_since(start_time);
    // the test will allocate 10^8 * 10^4 bytes in case if parameter will be evaluated for every row
    assert!(elapsed < std::time::Duration::from_millis(500));
}
