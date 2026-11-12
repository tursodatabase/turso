//! The functions that users define: what `CREATE FUNCTION` stores, and what
//! other connections and a later open of the database see.

use std::sync::Arc;

use crate::common::TempDatabase;
use turso_core::{Connection, Value};
use turso_pg::PgConnection;

fn text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.as_str().to_string(),
        other => panic!("expected text, got {other:?}"),
    }
}

/// The rows of `sqlite_schema` that store functions, as `(name, tbl_name,
/// rootpage, sql)`.
fn function_schema_rows(conn: &Arc<Connection>) -> Vec<(String, String, i64, String)> {
    conn.prepare(
        "SELECT name, tbl_name, rootpage, sql FROM sqlite_schema \
         WHERE type = 'pg_function_v1' ORDER BY name",
    )
    .unwrap()
    .run_collect_rows()
    .unwrap()
    .iter()
    .map(|row| {
        (
            text(&row[0]),
            text(&row[1]),
            row[2].as_int().unwrap(),
            text(&row[3]),
        )
    })
    .collect()
}

/// The class rows of functions, as `(oid, relname, relnamespace, relowner)`.
fn function_class_rows(conn: &Arc<Connection>) -> Vec<(i64, String, i64, i64)> {
    conn.prepare(
        "SELECT oid, relname, relnamespace, relowner FROM __turso_internal_classes \
         WHERE relkind = 'function' ORDER BY oid",
    )
    .unwrap()
    .run_collect_rows()
    .unwrap()
    .iter()
    .map(|row| {
        (
            row[0].as_int().unwrap(),
            text(&row[1]),
            row[2].as_int().unwrap(),
            row[3].as_int().unwrap(),
        )
    })
    .collect()
}

/// `(oid, proname, prosrc)` of the functions that users defined.
fn user_functions(pg: &PgConnection) -> Vec<(i64, String, String)> {
    pg.prepare("SELECT oid, proname, prosrc FROM pg_proc WHERE oid >= 16384 ORDER BY oid")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .iter()
        .map(|row| (row[0].as_int().unwrap(), text(&row[1]), text(&row[2])))
        .collect()
}

#[test]
fn create_function_stores_a_schema_row_and_a_class_row() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    let create =
        "CREATE FUNCTION add_one(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1 + 1$$";

    pg.execute("CREATE SCHEMA app").unwrap();
    pg.execute(create).unwrap();
    pg.execute("CREATE FUNCTION app.pad(text, bigint) RETURNS text LANGUAGE sql AS $$SELECT $1$$")
        .unwrap();

    // Each row is named after the namespace and the signature of its
    // function, behind a prefix that no relation can have: public is
    // namespace 1 and app is namespace 2. The engine gives each row of a
    // dialect the same table name, which no table can have.
    assert_eq!(
        function_schema_rows(&conn),
        vec![
            (
                "__turso_ns1___turso_internal_function_add_one(int4)".to_string(),
                "__turso_internal_dialect_row".to_string(),
                0,
                create.to_string()
            ),
            (
                "__turso_ns2___turso_internal_function_pad(text,int8)".to_string(),
                "__turso_internal_dialect_row".to_string(),
                0,
                "CREATE FUNCTION app.pad(text, bigint) RETURNS text LANGUAGE sql AS $$SELECT $1$$"
                    .to_string()
            ),
        ]
    );
    assert_eq!(
        function_class_rows(&conn),
        vec![
            (
                16384,
                "__turso_internal_function_add_one(int4)".to_string(),
                1,
                0
            ),
            (
                16385,
                "__turso_internal_function_pad(text,int8)".to_string(),
                2,
                0
            ),
        ]
    );

    pg.execute("DROP FUNCTION add_one(integer)").unwrap();
    pg.execute("DROP FUNCTION app.pad").unwrap();
    assert_eq!(function_schema_rows(&conn), vec![]);
    assert_eq!(function_class_rows(&conn), vec![]);
}

#[test]
fn replace_writes_the_new_statement_and_keeps_the_class_row() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    let conn = db.connect_limbo();
    let replace = "CREATE OR REPLACE FUNCTION one() RETURNS integer LANGUAGE sql AS $$SELECT 2$$";

    pg.execute("CREATE FUNCTION one() RETURNS integer LANGUAGE sql AS $$SELECT 1$$")
        .unwrap();
    pg.execute(replace).unwrap();

    assert_eq!(
        function_schema_rows(&conn)
            .into_iter()
            .map(|row| row.3)
            .collect::<Vec<_>>(),
        vec![replace.to_string()]
    );
    assert_eq!(
        function_class_rows(&conn),
        vec![(16384, "__turso_internal_function_one()".to_string(), 1, 0)]
    );
}

#[test]
fn functions_survive_a_reopen() {
    let db = TempDatabase::builder().build();
    {
        let pg = db.connect_postgres();
        pg.execute("CREATE FUNCTION one() RETURNS integer LANGUAGE sql AS $$SELECT 1$$")
            .unwrap();
        pg.execute("CREATE FUNCTION two() RETURNS integer LANGUAGE sql AS $$SELECT 2$$")
            .unwrap();
        pg.execute("DROP FUNCTION one").unwrap();
        pg.close().unwrap();
    }

    let db = db.reopen();
    let pg = db.connect_postgres();
    assert_eq!(
        user_functions(&pg),
        vec![(16385, "two".to_string(), "SELECT 2".to_string())]
    );
    // The oid of a dropped function is not used again.
    pg.execute("CREATE FUNCTION three() RETURNS integer LANGUAGE sql AS $$SELECT 3$$")
        .unwrap();
    assert_eq!(
        user_functions(&pg).last(),
        Some(&(16386, "three".to_string(), "SELECT 3".to_string()))
    );
}

/// DROP TABLE and ALTER TABLE ... RENAME change each row of `sqlite_schema`
/// that has the name of the table.
#[test]
fn a_table_with_the_name_of_a_signature_does_not_change_the_function() {
    let db = TempDatabase::builder().build();
    {
        let pg = db.connect_postgres();
        pg.execute("CREATE TABLE \"one()\" (a integer)").unwrap();
        pg.execute("CREATE TABLE \"two()\" (a integer)").unwrap();
        pg.execute("CREATE FUNCTION one() RETURNS integer LANGUAGE sql AS $$SELECT 1$$")
            .unwrap();
        pg.execute("CREATE FUNCTION two() RETURNS integer LANGUAGE sql AS $$SELECT 2$$")
            .unwrap();
        pg.execute("DROP TABLE \"one()\"").unwrap();
        pg.execute("ALTER TABLE \"two()\" RENAME TO renamed")
            .unwrap();
        pg.close().unwrap();
    }

    let db = db.reopen();
    let pg = db.connect_postgres();
    assert_eq!(
        user_functions(&pg),
        vec![
            (16386, "one".to_string(), "SELECT 1".to_string()),
            (16387, "two".to_string(), "SELECT 2".to_string())
        ]
    );
}

#[test]
fn another_connection_sees_function_changes() {
    let db = TempDatabase::builder().build();
    let writer = db.connect_postgres();
    let reader = db.connect_postgres();
    assert_eq!(user_functions(&reader), vec![]);

    writer
        .execute("CREATE FUNCTION one() RETURNS integer LANGUAGE sql AS $$SELECT 1$$")
        .unwrap();
    assert_eq!(
        user_functions(&reader),
        vec![(16384, "one".to_string(), "SELECT 1".to_string())]
    );

    writer
        .execute("CREATE OR REPLACE FUNCTION one() RETURNS integer LANGUAGE sql AS $$SELECT 2$$")
        .unwrap();
    assert_eq!(
        user_functions(&reader),
        vec![(16384, "one".to_string(), "SELECT 2".to_string())]
    );

    writer.execute("DROP FUNCTION one()").unwrap();
    assert_eq!(user_functions(&reader), vec![]);
}

#[test]
fn an_open_transaction_hides_its_function_from_other_connections() {
    let db = TempDatabase::builder().build();
    let writer = db.connect_postgres();
    let reader = db.connect_postgres();

    writer.execute("BEGIN").unwrap();
    writer
        .execute("CREATE FUNCTION one() RETURNS integer LANGUAGE sql AS $$SELECT 1$$")
        .unwrap();
    assert_eq!(user_functions(&writer).len(), 1);
    assert_eq!(user_functions(&reader), vec![]);

    writer.execute("COMMIT").unwrap();
    assert_eq!(user_functions(&reader).len(), 1);
}

#[test]
fn a_role_that_is_not_a_superuser_cannot_change_functions() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    pg.execute("CREATE FUNCTION one() RETURNS integer LANGUAGE sql AS $$SELECT 1$$")
        .unwrap();
    pg.execute("CREATE ROLE alice").unwrap();
    pg.execute("SET ROLE alice").unwrap();

    let create = pg
        .execute("CREATE FUNCTION two() RETURNS integer LANGUAGE sql AS $$SELECT 2$$")
        .unwrap_err();
    assert_eq!(create.sqlstate, "42501");
    let replace = pg
        .execute("CREATE OR REPLACE FUNCTION one() RETURNS integer LANGUAGE sql AS $$SELECT 2$$")
        .unwrap_err();
    assert_eq!(replace.sqlstate, "42501");
    let drop = pg.execute("DROP FUNCTION one()").unwrap_err();
    assert_eq!(drop.sqlstate, "42501");
    assert_eq!(drop.to_string(), "must be owner of function one");
}
