//! The namespaces of the main database, as the engine keeps them for the
//! PostgreSQL dialect: how they are stored, reloaded, copied and looked up.
//! A new database has `public` as namespace 1, so the first schema a test
//! creates is namespace 2.

use std::path::Path;
use std::sync::Arc;

use crate::common::TempDatabase;
use turso_core::{Connection, NamespaceId, Qualifier, Value};

/// Runs `sql`, which returns one integer, to its end, so that what it
/// writes is committed, and returns the integer.
fn query_i64(conn: &Arc<Connection>, sql: &str) -> i64 {
    let rows = conn.prepare(sql).unwrap().run_collect_rows().unwrap();
    match rows.first().and_then(|row| row.first()) {
        Some(value) => value
            .as_int()
            .unwrap_or_else(|| panic!("expected an integer, got {value:?} for: {sql}")),
        None => panic!("expected a row for: {sql}"),
    }
}

fn reopen(path: &Path) -> TempDatabase {
    TempDatabase::builder()
        .with_db_name(path.to_str().unwrap())
        .build()
}

#[turso_macros::test]
fn a_namespace_is_part_of_its_transaction_and_seen_by_other_connections(db: TempDatabase) {
    let conn = db.connect_limbo();
    let other = db.connect_limbo();

    conn.execute("BEGIN").unwrap();
    conn.create_namespace("app").unwrap();
    conn.execute("CREATE TABLE app.t(x INTEGER); INSERT INTO app.t VALUES(5)")
        .unwrap();
    assert_eq!(query_i64(&conn, "SELECT x FROM app.t"), 5);
    assert_eq!(other.qualifier("app"), None);
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(conn.qualifier("app"), None);
    assert!(conn.prepare("SELECT x FROM app.t").is_err());

    conn.execute("BEGIN").unwrap();
    conn.create_namespace("app").unwrap();
    conn.execute("CREATE TABLE app.t(x INTEGER); INSERT INTO app.t VALUES(5)")
        .unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        other.qualifier("app"),
        Some(Qualifier::in_main("app", NamespaceId::new(2)))
    );
    assert_eq!(query_i64(&other, "SELECT x FROM app.t"), 5);

    let path = db.path.clone();
    drop(other);
    drop(conn);
    drop(db);
    let db = reopen(&path);
    let reopened = db.connect_limbo();
    assert_eq!(
        reopened.qualifier("app"),
        Some(Qualifier::in_main("app", NamespaceId::new(2)))
    );
    assert_eq!(query_i64(&reopened, "SELECT x FROM app.t"), 5);
}

#[turso_macros::test]
fn a_namespace_keeps_the_objects_stored_under_its_id(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE public.t(x INTEGER); INSERT INTO public.t VALUES(1)")
        .unwrap();
    let app = conn.create_namespace("app").unwrap();
    assert_eq!(app, Qualifier::in_main("app", NamespaceId::new(2)));
    assert_eq!(conn.qualifier("app"), Some(app));
    assert!(conn.create_namespace("app").is_err());
    assert!(conn.create_namespace("main").is_err());
    conn.execute("CREATE TABLE app.t(x INTEGER); INSERT INTO app.t VALUES(5)")
        .unwrap();

    assert_eq!(query_i64(&conn, "SELECT x FROM app.t"), 5);
    assert_eq!(query_i64(&conn, "SELECT x FROM public.t"), 1);
    assert_eq!(
        query_i64(
            &conn,
            "SELECT count(*) FROM sqlite_schema WHERE name = '__turso_ns2_t'"
        ),
        1
    );
    assert!(conn.prepare("SELECT x FROM __turso_ns2_t").is_err());
    conn.execute("INSERT INTO app.t VALUES(6)").unwrap();
    conn.execute("UPDATE app.t SET x = x + 10 WHERE x = 6")
        .unwrap();
    assert_eq!(query_i64(&conn, "SELECT sum(x) FROM app.t"), 21);
    conn.execute("DELETE FROM app.t WHERE x = 16").unwrap();
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM app.t"), 1);
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM public.t"), 1);
    assert!(conn
        .execute("CREATE TABLE public.__turso_ns2_u(x INTEGER)")
        .is_err());
    assert!(conn
        .execute("CREATE TABLE app.__turso_ns2_u(x INTEGER)")
        .is_err());
    assert_eq!(
        query_i64(
            &conn,
            "SELECT id FROM __turso_internal_namespaces WHERE name = 'app'"
        ),
        2
    );
    assert!(conn.drop_namespace("shop").is_err());

    conn.execute("DROP TABLE app.t").unwrap();
    assert!(conn.prepare("SELECT x FROM app.t").is_err());
    assert_eq!(
        query_i64(
            &conn,
            "SELECT count(*) FROM sqlite_schema WHERE name = '__turso_ns2_t'"
        ),
        0
    );
    assert_eq!(query_i64(&conn, "SELECT x FROM public.t"), 1);
    conn.drop_namespace("app").unwrap();
    assert_eq!(conn.qualifier("app"), None);
    assert!(conn.prepare("SELECT x FROM app.t").is_err());
    assert!(conn.drop_namespace("app").is_err());
    assert!(conn.drop_namespace("main").is_err());
    let shop = conn.create_namespace("shop").unwrap();
    assert_eq!(shop.target.id, NamespaceId::new(2));
}

#[turso_macros::test]
fn an_id_whose_objects_are_still_stored_is_not_given_to_a_new_namespace(db: TempDatabase) {
    let conn = db.connect_limbo();
    let app = conn.create_namespace("app").unwrap();
    assert_eq!(app.target.id, NamespaceId::new(2));
    conn.execute("CREATE TABLE app.t(x INTEGER); INSERT INTO app.t VALUES(5)")
        .unwrap();
    conn.drop_namespace("app").unwrap();

    let shop = conn.create_namespace("shop").unwrap();
    assert_eq!(shop.target.id, NamespaceId::new(3));
    assert!(conn.prepare("SELECT x FROM shop.t").is_err());
}

#[turso_macros::test]
fn the_search_path_finds_objects_through_a_namespace(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.create_namespace("app").unwrap();
    conn.execute("CREATE TABLE app.t(x INTEGER); INSERT INTO app.t VALUES(5)")
        .unwrap();
    conn.execute("CREATE TABLE public.t(x INTEGER); INSERT INTO public.t VALUES(1)")
        .unwrap();
    conn.execute("CREATE SEQUENCE app.s START WITH 100")
        .unwrap();
    conn.execute("CREATE SEQUENCE public.s START WITH 1")
        .unwrap();

    conn.set_search_path(Some(vec!["app".to_string()]));
    assert_eq!(query_i64(&conn, "SELECT x FROM t"), 5);
    assert_eq!(query_i64(&conn, "SELECT nextval('s')"), 100);
    assert_eq!(query_i64(&conn, "SELECT currval('s')"), 100);
    assert_eq!(query_i64(&conn, "SELECT nextval('app.s')"), 101);
    assert_eq!(query_i64(&conn, "SELECT nextval('public.s')"), 1);
    assert_eq!(conn.search_order()[0].name, "app");

    conn.set_search_path(Some(vec!["public".to_string(), "app".to_string()]));
    assert_eq!(query_i64(&conn, "SELECT x FROM t"), 1);
    assert_eq!(query_i64(&conn, "SELECT nextval('s')"), 2);
    conn.execute("DROP TABLE t").unwrap();
    assert_eq!(query_i64(&conn, "SELECT x FROM t"), 5);

    conn.set_search_path(Some(vec!["app".to_string(), "public".to_string()]));
    conn.execute("DROP SEQUENCE s").unwrap();
    assert!(conn.prepare("SELECT nextval('app.s')").is_err());
    assert_eq!(query_i64(&conn, "SELECT nextval('s')"), 3);
}

#[turso_macros::test]
fn objects_of_a_namespace_survive_reopening_and_renaming(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.create_namespace("app").unwrap();
    conn.execute("CREATE TABLE app.t(x INTEGER PRIMARY KEY, y TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX t_y ON app.t(y)").unwrap();
    conn.execute("CREATE VIEW app.v AS SELECT x FROM app.t")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER app.trg AFTER INSERT ON t BEGIN INSERT INTO t VALUES(new.x + 100, 'copy'); END",
    )
    .unwrap();
    conn.execute("INSERT INTO app.t VALUES(1, 'a')").unwrap();
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM app.t"), 2);
    assert_eq!(query_i64(&conn, "SELECT sum(x) FROM app.v"), 102);

    let path = db.path.clone();
    drop(conn);
    drop(db);
    let db = reopen(&path);
    let conn = db.connect_limbo();
    assert_eq!(query_i64(&conn, "SELECT sum(x) FROM app.v"), 102);
    assert_eq!(
        query_i64(&conn, "SELECT count(*) FROM app.t WHERE y = 'copy'"),
        1
    );
    conn.execute("ALTER TABLE app.t RENAME TO u").unwrap();
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM app.u"), 2);
    conn.execute("INSERT INTO app.u VALUES(2, 'b')").unwrap();
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM app.u"), 4);
    assert_eq!(
        query_i64(
            &conn,
            "SELECT count(*) FROM sqlite_schema WHERE tbl_name = '__turso_ns2_u'"
        ),
        3
    );
    conn.execute("DROP INDEX app.t_y").unwrap();
    conn.execute("DROP TRIGGER app.trg").unwrap();
    conn.execute("DROP VIEW app.v").unwrap();
    conn.execute("DROP TABLE app.u").unwrap();
    assert_eq!(
        query_i64(
            &conn,
            "SELECT count(*) FROM sqlite_schema WHERE name LIKE '__turso_ns2_%'"
        ),
        0
    );
}

#[turso_macros::test]
fn vacuum_into_keeps_the_ids_of_namespaces(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.create_namespace("app").unwrap();
    let shop = conn.create_namespace("shop").unwrap();
    assert_eq!(shop.target.id, NamespaceId::new(3));
    conn.execute("CREATE TABLE shop.t(x INTEGER); INSERT INTO shop.t VALUES(7)")
        .unwrap();
    conn.drop_namespace("app").unwrap();
    let copy = db.path.with_file_name(format!(
        "{}-copy.db",
        db.path.file_stem().unwrap().to_str().unwrap()
    ));
    conn.execute(format!("VACUUM INTO '{}'", copy.display()))
        .unwrap();

    let copied_db = reopen(&copy);
    let copied = copied_db.connect_limbo();
    assert_eq!(
        copied.qualifier("shop"),
        Some(Qualifier::in_main("shop", NamespaceId::new(3)))
    );
    assert_eq!(query_i64(&copied, "SELECT x FROM shop.t"), 7);
}

#[turso_macros::test]
fn pragma_table_info_finds_a_view_of_a_namespace_through_the_search_path(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.create_namespace("app").unwrap();
    conn.set_search_path(Some(vec!["app".to_string()]));
    conn.execute("CREATE TABLE t(x INTEGER, y TEXT); CREATE VIEW v AS SELECT x, y FROM t")
        .unwrap();
    let names = |sql: &str| -> Vec<String> {
        let mut stmt = conn.prepare(sql).unwrap();
        stmt.run_collect_rows()
            .unwrap()
            .into_iter()
            .map(|row| row[1].to_string())
            .collect()
    };
    assert_eq!(names("PRAGMA table_info('v')"), ["x", "y"]);
    assert_eq!(names("PRAGMA table_info('t')"), ["x", "y"]);
    let mut stmt = conn
        .prepare("SELECT name FROM pragma_table_info('v') ORDER BY cid")
        .unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap(),
        vec![vec![Value::build_text("x")], vec![Value::build_text("y")]]
    );
}
