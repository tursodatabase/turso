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
        // Aliases used before their output is evaluated expand in place.
        "SELECT value + 1 AS x FROM items WHERE x > 15 ORDER BY x",
        "SELECT extra AS e, count(*) FROM items GROUP BY e ORDER BY e",
        "SELECT extra, sum(value) FILTER (WHERE value > 15) AS s FROM items GROUP BY extra HAVING s > 0",
        "SELECT extra AS e, count(*) FROM items GROUP BY 1 ORDER BY 1",
        "SELECT json_valid('{a:1}', 6), json_valid('{}')",
        // A subquery column is named after the column it reads, not `i.value`.
        "SELECT d.value FROM (SELECT i.value FROM items i WHERE i.id < 3) AS d ORDER BY 1",
        "WITH s AS (SELECT i.id, t.tag FROM items i JOIN tags t ON t.item_id = i.id) \
         SELECT s.id, s.tag FROM s ORDER BY 1, 2",
        "SELECT d.id FROM (SELECT items.rowid FROM items) AS d WHERE d.id < 3 ORDER BY 1",
        "SELECT * FROM (SELECT i.value FROM items i WHERE i.id < 3) ORDER BY 1",
        "WITH s AS (SELECT id FROM items) SELECT s.id, x.id FROM s, s AS x WHERE s.id = x.id",
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
        // Several windows nest one layer per window, first window outermost.
        "SELECT id, row_number() OVER (ORDER BY value), rank() OVER (ORDER BY extra) FROM items",
        "SELECT id, lag(value) OVER (ORDER BY id), lead(value) OVER (ORDER BY id) FROM items",
        "SELECT id, sum(value) OVER (PARTITION BY extra), count(*) OVER (ORDER BY id DESC), \
         max(value) OVER () FROM items ORDER BY id",
        "SELECT extra, count(*), rank() OVER (ORDER BY count(*)), \
         sum(sum(value)) OVER (ORDER BY extra) FROM items GROUP BY extra",
        "SELECT id, sum(value) OVER (ORDER BY id) + row_number() OVER (ORDER BY value DESC, id) \
         FROM items LIMIT 3 OFFSET 1",
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
        // Group output reads key expressions from the key and the outer
        // columns of correlated subqueries from the captured group row.
        "SELECT (SELECT tag FROM tags WHERE item_id = items.id LIMIT 1) AS t, sum(value) \
         FROM items GROUP BY (SELECT tag FROM tags WHERE item_id = items.id LIMIT 1)",
        "SELECT extra, (SELECT count(*) FROM tags WHERE item_id = items.id) FROM items GROUP BY extra",
        "SELECT count(*), (SELECT items.value + 1) FROM items WHERE id = 3",
        "SELECT value + 1, count(*) FROM items GROUP BY value + 1 HAVING value + 1 > 11 ORDER BY value + 1",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "rows differ for {sql}"
        );
    }
}

#[test]
fn hir_views_expand_like_legacy() {
    let db = setup();
    let [legacy, hir] = connections(&db);
    for statement in [
        "CREATE VIEW cheap AS SELECT id, value FROM items WHERE value < 25",
        "CREATE VIEW renamed(n, label) AS SELECT value * 10, upper(extra) FROM items",
        "CREATE VIEW nested AS SELECT n FROM renamed WHERE n > 100",
        "CREATE VIEW loop_a AS SELECT * FROM items",
    ] {
        legacy.execute(statement).unwrap();
    }
    for sql in [
        "SELECT * FROM cheap ORDER BY id",
        "SELECT * FROM renamed ORDER BY n DESC",
        "SELECT r.n, c.id FROM renamed r JOIN cheap c ON r.n = c.value * 10 ORDER BY 1, 2",
        "SELECT * FROM nested ORDER BY n",
        // The view's SELECT reads the table, not the statement's CTE.
        "WITH items AS (SELECT 99 AS id, 99 AS value) SELECT * FROM cheap ORDER BY id",
        "SELECT count(*), (SELECT max(n) FROM renamed) FROM cheap",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "rows differ for {sql}"
        );
        assert_eq!(column_names(&hir, sql), column_names(&legacy, sql), "{sql}");
    }

    legacy.execute("DROP VIEW loop_a").unwrap();
    legacy
        .execute("CREATE VIEW loop_b AS SELECT 1 AS one")
        .unwrap();
    legacy
        .execute("CREATE VIEW loop_a AS SELECT * FROM loop_b")
        .unwrap();
    legacy.execute("DROP VIEW loop_b").unwrap();
    legacy
        .execute("CREATE VIEW loop_b AS SELECT * FROM loop_a")
        .unwrap();
    for conn in [&legacy, &hir] {
        let error = conn
            .prepare("SELECT * FROM loop_a")
            .err()
            .expect("circular view fails");
        assert!(error.to_string().contains("circularly defined"), "{error}");
    }
}

#[test]
fn hir_right_and_full_joins_match_legacy_rows() {
    let db = setup();
    let [legacy, hir] = connections(&db);
    for sql in [
        "SELECT i.id, t.tag FROM items i RIGHT JOIN tags t ON t.item_id = i.id ORDER BY 2, 1",
        "SELECT i.id, t.tag FROM items i FULL JOIN tags t ON t.item_id = i.id ORDER BY 1, 2",
        "SELECT i.id, t.tag FROM items i FULL JOIN tags t ON t.item_id = i.id \
         WHERE i.id IS NULL OR t.tag IS NULL ORDER BY 1, 2",
        "SELECT i.id, t.tag FROM items i RIGHT JOIN tags t ON t.item_id < i.id AND i.value > 25 \
         ORDER BY 2, 1",
        "SELECT count(*), count(i.id), count(t.tag) FROM items i FULL JOIN tags t ON t.item_id = i.id",
        "SELECT t.tag, count(i.id) FROM items i RIGHT JOIN tags t ON t.item_id = i.id \
         GROUP BY t.tag ORDER BY 1",
        "SELECT i.id, t.tag FROM items i FULL JOIN tags t ON 1 WHERE i.id = 1 ORDER BY 1, 2",
        "SELECT i.id, t.tag, j.id FROM items i FULL JOIN tags t ON t.item_id = i.id \
         LEFT JOIN items j ON j.id = t.item_id ORDER BY 1, 2, 3",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "rows differ for {sql}"
        );
    }

    // HIR must match SQLite here. Legacy returns wrong rows for the last two.
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch(SETUP).unwrap();
    for sql in [
        // The unmatched-row pass checks WHERE terms only, not inner ON terms.
        "SELECT i.id, j.id, t.tag FROM items i JOIN items j ON j.id = i.id \
         FULL JOIN tags t ON t.item_id = i.id ORDER BY 1, 2, 3",
        // A WHERE term on the left side runs after the match is recorded, so
        // every tag counts as matched and no NULL-extended row appears.
        "SELECT i.id, t.tag FROM items i RIGHT JOIN tags t ON 1 WHERE i.id IS NULL",
        // Legacy puts the inner join's columns before the RIGHT JOIN's.
        "SELECT * FROM items i RIGHT JOIN tags t ON t.item_id = i.id \
         JOIN items j ON j.id = t.item_id ORDER BY t.tag, i.id",
        // Legacy emits extra rows when a comma join comes before FULL JOIN.
        "SELECT i.id, j.id, t.tag FROM items i, items j FULL JOIN tags t ON t.item_id = j.id \
         WHERE i.id = 1 ORDER BY 1, 2, 3",
    ] {
        let mut stmt = sqlite.prepare(sql).unwrap();
        let columns = stmt.column_count();
        let expected: Vec<Vec<rusqlite::types::Value>> = stmt
            .query_map([], |row| {
                (0..columns).map(|column| row.get(column)).collect()
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            expected,
            "rows differ for {sql}"
        );
    }

    for (sql, message) in [
        (
            "SELECT * FROM items a FULL JOIN items b ON a.id = b.id FULL JOIN tags t ON t.item_id = b.id",
            "FULL OUTER JOIN chaining is not yet supported",
        ),
        (
            "SELECT * FROM items a FULL JOIN items b USING (id)",
            "FULL OUTER JOIN requires an equality condition in the ON clause",
        ),
    ] {
        for conn in [&legacy, &hir] {
            let error = conn.prepare(sql).err().expect("FULL JOIN shape fails");
            assert!(error.to_string().contains(message), "{error}");
        }
    }
}
