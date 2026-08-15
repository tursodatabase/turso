//! Tests for the SQL surface.
//!
//! Run with: wasm-pack test --node
//!
//! These run under Node because they touch no browser API: they use the
//! in-memory backend and js_sys, nothing else. The OPFS path needs a Web
//! Worker for sync access handles, which `wasm-bindgen-test` cannot provide,
//! so `example/` exercises that path in a real browser instead.

use turso_wasm::Database;
use wasm_bindgen_test::*;

fn text(row: &JsValue, key: &str) -> String {
    js_sys::Reflect::get(row, &JsValue::from_str(key))
        .unwrap()
        .as_string()
        .unwrap()
}

fn number(row: &JsValue, key: &str) -> f64 {
    js_sys::Reflect::get(row, &JsValue::from_str(key))
        .unwrap()
        .as_f64()
        .unwrap()
}

use wasm_bindgen::JsValue;

#[wasm_bindgen_test]
fn runs_a_query_that_needs_no_table() {
    let db = Database::in_memory().unwrap();
    let rows = db.query("SELECT 1 + 1 AS total").unwrap();
    assert_eq!(rows.length(), 1);
    assert_eq!(number(&rows.get(0), "total"), 2.0);
}

#[wasm_bindgen_test]
fn creates_a_table_and_reads_it_back() {
    let db = Database::in_memory().unwrap();
    db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.exec("INSERT INTO users (name) VALUES ('alice')")
        .unwrap();
    db.exec("INSERT INTO users (name) VALUES ('bob')").unwrap();

    let rows = db.query("SELECT id, name FROM users ORDER BY id").unwrap();
    assert_eq!(rows.length(), 2);
    assert_eq!(text(&rows.get(0), "name"), "alice");
    assert_eq!(text(&rows.get(1), "name"), "bob");
    assert_eq!(number(&rows.get(1), "id"), 2.0);
}

#[wasm_bindgen_test]
fn reports_how_many_rows_changed() {
    let db = Database::in_memory().unwrap();
    db.exec("CREATE TABLE t (v INTEGER)").unwrap();
    db.exec("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    assert_eq!(db.exec("UPDATE t SET v = v + 1").unwrap(), 3);
}

#[wasm_bindgen_test]
fn tracks_the_last_insert_rowid() {
    let db = Database::in_memory().unwrap();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.exec("INSERT INTO t (v) VALUES (10)").unwrap();
    assert_eq!(db.last_insert_row_id(), 1);
    db.exec("INSERT INTO t (v) VALUES (20)").unwrap();
    assert_eq!(db.last_insert_row_id(), 2);
}

#[wasm_bindgen_test]
fn round_trips_every_value_type() {
    let db = Database::in_memory().unwrap();
    db.exec("CREATE TABLE t (i INTEGER, f REAL, s TEXT, b BLOB, n TEXT)")
        .unwrap();
    db.exec("INSERT INTO t VALUES (42, 3.5, 'hello', x'00ff', NULL)")
        .unwrap();

    let row = db.query("SELECT i, f, s, b, n FROM t").unwrap().get(0);
    assert_eq!(number(&row, "i"), 42.0);
    assert_eq!(number(&row, "f"), 3.5);
    assert_eq!(text(&row, "s"), "hello");
    assert!(js_sys::Reflect::get(&row, &JsValue::from_str("n"))
        .unwrap()
        .is_null());

    let blob = js_sys::Reflect::get(&row, &JsValue::from_str("b")).unwrap();
    let blob = js_sys::Uint8Array::new(&blob);
    assert_eq!(blob.to_vec(), vec![0x00, 0xff]);
}

#[wasm_bindgen_test]
fn commits_and_rolls_back() {
    let db = Database::in_memory().unwrap();
    db.exec("CREATE TABLE t (v INTEGER)").unwrap();
    db.exec("BEGIN").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    db.exec("COMMIT").unwrap();
    db.exec("BEGIN").unwrap();
    db.exec("INSERT INTO t VALUES (2)").unwrap();
    db.exec("ROLLBACK").unwrap();

    let rows = db.query("SELECT v FROM t").unwrap();
    assert_eq!(rows.length(), 1);
    assert_eq!(number(&rows.get(0), "v"), 1.0);
}

#[wasm_bindgen_test]
fn reports_a_syntax_error_instead_of_panicking() {
    let db = Database::in_memory().unwrap();
    assert!(db.query("SELECT FROM WHERE").is_err());
}

#[wasm_bindgen_test]
fn keeps_integers_wider_than_a_js_number_exact() {
    let db = Database::in_memory().unwrap();
    db.exec("CREATE TABLE t (v INTEGER)").unwrap();
    db.exec("INSERT INTO t VALUES (9007199254740993)").unwrap();

    // Beyond Number.MAX_SAFE_INTEGER, so this must arrive as a BigInt rather
    // than a silently rounded double.
    let value = js_sys::Reflect::get(
        &db.query("SELECT v FROM t").unwrap().get(0),
        &JsValue::from_str("v"),
    )
    .unwrap();
    assert_eq!(value.js_typeof().as_string().unwrap(), "bigint");
}
