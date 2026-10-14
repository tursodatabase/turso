//! Top-level SELECT prepared through semantic HIR must return the same rows
//! and column metadata as the legacy planner.

use crate::common::{limbo_exec_rows, TempDatabase};

const SETUP: &str = "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER, extra TEXT);
CREATE INDEX items_value ON items(value);
INSERT INTO items VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b'), (4, NULL, 'c'), (5, 50, 'c'), (6, 20, 'b');
CREATE TABLE tags(item_id INTEGER, tag TEXT);
INSERT INTO tags VALUES (1, 'x'), (1, 'y'), (3, 'x'), (9, 'z');";

type ColumnMetadata = (String, Option<String>, Option<String>);

fn setup() -> TempDatabase {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    for statement in SETUP.split(';').filter(|sql| !sql.trim().is_empty()) {
        conn.execute(statement).unwrap();
    }
    db
}

fn column_metadata(
    conn: &std::sync::Arc<turso_core::Connection>,
    sql: &str,
) -> Vec<ColumnMetadata> {
    let stmt = conn.prepare(sql).unwrap();
    (0..stmt.num_columns())
        .map(|column| {
            (
                stmt.get_column_name(column).to_string(),
                stmt.get_column_table_name(column)
                    .map(|name| name.to_string()),
                stmt.get_column_decltype(column),
            )
        })
        .collect()
}

fn column_names(conn: &std::sync::Arc<turso_core::Connection>, sql: &str) -> Vec<String> {
    column_metadata(conn, sql)
        .into_iter()
        .map(|(name, _, _)| name)
        .collect()
}

fn connections(db: &TempDatabase) -> [std::sync::Arc<turso_core::Connection>; 2] {
    let legacy = db.connect_limbo();
    legacy.set_experimental_hir(false);
    let hir = db.connect_limbo();
    hir.set_experimental_hir(true);
    [legacy, hir]
}

#[test]
fn hir_select_matches_legacy_rows_and_columns() {
    let db = setup();
    let [legacy, hir] = connections(&db);
    for sql in [
        "SELECT 1, 'x' AS label",
        "SELECT * FROM items",
        "SELECT rowid, items.id, value AS v FROM items WHERE value > 15 ORDER BY value DESC, id",
        "SELECT DISTINCT extra FROM items ORDER BY 1 LIMIT 2 OFFSET 1",
        "SELECT extra, count(*), sum(value) FROM items GROUP BY extra HAVING count(*) > 1",
        "SELECT count(*), max(value) FROM items",
        "SELECT a.id, b.id FROM items a JOIN items b ON a.value = b.value + 10 ORDER BY 1, 2",
        "SELECT a.id, b.id FROM items a LEFT JOIN items b ON b.value = a.value + 10 ORDER BY 1, 2",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "rows differ for {sql}"
        );
        assert_eq!(
            column_metadata(&hir, sql),
            column_metadata(&legacy, sql),
            "columns differ for {sql}"
        );
    }
}

#[test]
fn hir_window_select_matches_legacy_rows() {
    let db = setup();
    let [legacy, hir] = connections(&db);
    for sql in [
        "SELECT id, sum(value) OVER (PARTITION BY extra ORDER BY id) FROM items",
        "SELECT id, row_number() OVER () FROM items",
        "SELECT id, rank() OVER (ORDER BY value DESC NULLS FIRST) FROM items ORDER BY id LIMIT 4",
        "SELECT id, sum(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM items",
        "SELECT id, sum(value) OVER (ORDER BY value RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) FROM items",
        "SELECT id, count(*) OVER (ORDER BY value GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM items",
        "SELECT id, lag(value, 2) OVER (ORDER BY id), row_number() OVER (ORDER BY id) FROM items",
        "SELECT id, lead(value) OVER (ORDER BY id) FROM items",
        "SELECT id, sum(value) OVER w, avg(value) OVER (ORDER BY id) FROM items WINDOW w AS (ORDER BY id)",
        "SELECT id, first_value(id) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM items",
        "SELECT extra, count(*), sum(count(*)) OVER (PARTITION BY count(*)) FROM items GROUP BY extra",
        "SELECT DISTINCT extra, count(*) OVER (PARTITION BY extra) FROM items",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "rows differ for {sql}"
        );
        assert_eq!(column_names(&hir, sql), column_names(&legacy, sql), "{sql}");
    }

    // Legacy reports its internal window subquery as the table of every
    // output, and BLOB as the declared type of `count(*)`. HIR reports the
    // real source column, and nothing for an expression, like SQLite.
    assert_eq!(
        column_metadata(
            &hir,
            "SELECT extra, count(*) OVER (PARTITION BY extra) FROM items",
        ),
        [
            (
                "extra".to_string(),
                Some("items".to_string()),
                Some("TEXT".to_string())
            ),
            ("count(*) OVER (PARTITION BY extra)".to_string(), None, None),
        ]
    );
}

#[test]
fn hir_expression_subqueries_match_legacy_rows() {
    let db = setup();
    let [legacy, hir] = connections(&db);
    for sql in [
        "SELECT (SELECT max(value) FROM items)",
        "SELECT id, (SELECT count(*) FROM tags WHERE item_id = items.id) FROM items",
        "SELECT id FROM items WHERE EXISTS (SELECT 1 FROM tags WHERE item_id = id)",
        "SELECT id FROM items WHERE NOT EXISTS (SELECT 1 FROM tags WHERE item_id = id)",
        "SELECT id FROM items WHERE id IN (SELECT item_id FROM tags)",
        "SELECT id FROM items WHERE id NOT IN (SELECT item_id FROM tags)",
        "SELECT id, value IN (SELECT value FROM items WHERE extra = 'a') FROM items",
        "SELECT a.id, b.id FROM items a, items b \
         WHERE b.id IN (SELECT item_id FROM tags WHERE item_id = a.id)",
        "SELECT (SELECT 1, 2) = (1, 2)",
        "SELECT extra, (SELECT count(*) FROM tags WHERE tag = extra) FROM items GROUP BY extra",
        "SELECT id FROM items WHERE value > (SELECT avg(value) FROM items) ORDER BY id",
        "SELECT id, sum(value) OVER (ORDER BY id) FROM items WHERE id IN (SELECT item_id FROM tags)",
        "SELECT id FROM items LIMIT (SELECT 2)",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "rows differ for {sql}"
        );
    }
}
