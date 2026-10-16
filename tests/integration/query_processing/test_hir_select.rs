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
        // Groups are sorted in the ORDER BY direction, so ties come out like
        // legacy and SQLite.
        "SELECT extra, count(*) FROM items GROUP BY extra ORDER BY count(*) DESC",
        "SELECT extra, value, count(*) FROM items GROUP BY extra, value ORDER BY value DESC",
        "SELECT json_valid('{a:1}', 6), json_valid('{}')",
        "SELECT subtype(json('{}')), subtype('{}'), subtype(1)",
        // Blocks without FROM run GROUP BY and HAVING over their one row.
        "SELECT 1 ORDER BY sum((SELECT 42))",
        "SELECT 1 GROUP BY 1 HAVING count(*) FILTER (WHERE 1 IN (SELECT 1)) = 1",
        "SELECT 1 GROUP BY 1 HAVING count(*) FILTER (WHERE 1 IN (SELECT 2)) = 1",
        "SELECT 5 AS x, count(*) GROUP BY x",
        "SELECT 5 AS x, count(*) WHERE 0 GROUP BY x",
        "SELECT count(*) HAVING count(*) > 1",
        // A subquery column is named after the column it reads, not `i.value`.
        "SELECT d.value FROM (SELECT i.value FROM items i WHERE i.id < 3) AS d ORDER BY 1",
        "WITH s AS (SELECT i.id, t.tag FROM items i JOIN tags t ON t.item_id = i.id) \
         SELECT s.id, s.tag FROM s ORDER BY 1, 2",
        "SELECT d.id FROM (SELECT items.rowid FROM items) AS d WHERE d.id < 3 ORDER BY 1",
        "SELECT * FROM (SELECT i.value FROM items i WHERE i.id < 3) ORDER BY 1",
        // A compound ORDER BY term matches an output expression of any arm.
        "SELECT a.id FROM items a UNION ALL SELECT b.value FROM items b ORDER BY b.value",
        "SELECT value + 1 FROM items UNION SELECT id FROM tags, items ORDER BY value + 1",
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
        "SELECT id, row_number(*) OVER (ORDER BY id), rank(*) OVER (ORDER BY extra) FROM items",
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
        // Outer reads in a nested query make the enclosing subquery
        // correlated too, so it runs again for every outer row.
        "SELECT id, (SELECT count(*) FROM (SELECT tag FROM tags WHERE tags.item_id <= items.id)) \
         FROM items ORDER BY id",
        "SELECT id, (SELECT (SELECT items.value + 1)) FROM items ORDER BY id",
        "SELECT id FROM items WHERE EXISTS (SELECT 1 FROM tags \
         WHERE EXISTS (SELECT 1 WHERE tags.item_id = items.id)) ORDER BY id",
        "SELECT id FROM items WHERE EXISTS \
         (SELECT 1 FROM (SELECT tag FROM tags WHERE tags.item_id = items.id)) ORDER BY id",
        // A subquery column computed from a literal has no affinity, so 2
        // stays an integer. (Legacy reports BLOB as its declared type, SQLite
        // and HIR none, so only rows are compared.)
        "SELECT x, typeof(x) FROM (SELECT 1.5 AS x UNION ALL SELECT 2)",
        // A compound subquery column's affinity combines all arms.
        "SELECT typeof(x), x = 2 FROM (SELECT CAST(2 AS TEXT) AS x UNION ALL SELECT 2.0)",
        "SELECT typeof(x), x = '10' FROM (SELECT 5.0 AS x UNION ALL SELECT value FROM items \
         WHERE id = 1)",
        // EXISTS drops DISTINCT and never evaluates ORDER BY; OFFSET counts
        // plain rows.
        "SELECT EXISTS (SELECT extra FROM items ORDER BY abs(-9223372036854775807 - 1))",
        "SELECT EXISTS (SELECT DISTINCT extra FROM items ORDER BY extra LIMIT 9 OFFSET 5)",
        // A recursive CTE that reads an outer column runs again per outer row.
        "SELECT id, (WITH RECURSIVE c(x) AS (SELECT items.id UNION ALL SELECT x + 1 FROM c \
         WHERE x < 4) SELECT count(*) FROM c) FROM items ORDER BY id",
        // A recursive CTE's reference to itself has no affinity, so '2' < 3
        // is false and the recursion stops.
        "WITH RECURSIVE c(x) AS (SELECT value FROM items WHERE id = 1 UNION ALL \
         SELECT '2' FROM c WHERE x < 3 LIMIT 10) SELECT count(*) FROM c",
        // In a recursive step, the CTE's current row is NULL on the
        // NULL-extended side of a LEFT JOIN and in a RIGHT JOIN's unmatched
        // rows.
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT c.x + 1 FROM items \
         LEFT JOIN c ON items.id = 1 WHERE c.x < 3) SELECT * FROM c",
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT tags.item_id FROM c \
         RIGHT JOIN tags ON tags.item_id = c.x + 2 WHERE c.x < 3 LIMIT 4) SELECT x FROM c",
        // FROM subqueries and CTEs stream rows, so an outer LIMIT stops an
        // endless recursive CTE.
        "WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM seq) \
         SELECT x FROM seq LIMIT 5",
        "SELECT a.id, b.v FROM items a JOIN (SELECT id + 1 AS k, value AS v FROM items) b \
         ON b.k = a.id ORDER BY 1",
        // The index on `value` does not cover `items` here: the subquery reads
        // `items.extra`.
        "SELECT value FROM items WHERE value > 15 \
         AND EXISTS (SELECT 1 FROM tags WHERE tags.tag <> items.extra) ORDER BY value",
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

    // HIR must match SQLite here. Legacy returns wrong rows for the last two
    // and rejects the FULL JOIN of a subquery.
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch(SETUP).unwrap();
    for sql in [
        // The unmatched-row pass checks WHERE terms only, not inner ON terms.
        "SELECT i.id, j.id, t.tag FROM items i JOIN items j ON j.id = i.id \
         FULL JOIN tags t ON t.item_id = i.id ORDER BY 1, 2, 3",
        // A WHERE term on the left side runs after the match is recorded, so
        // every tag counts as matched and no NULL-extended row appears.
        "SELECT i.id, t.tag FROM items i RIGHT JOIN tags t ON 1 WHERE i.id IS NULL",
        // A materialized subquery on the right is scanned again by rowid.
        "SELECT i.id, d.item_id FROM items i \
         FULL JOIN (SELECT DISTINCT item_id FROM tags) AS d ON d.item_id = i.id ORDER BY 1, 2",
        "SELECT i.extra, d.tag FROM (SELECT extra FROM items WHERE value > 15) AS i \
         RIGHT JOIN (SELECT tag FROM tags) AS d ON d.tag > i.extra ORDER BY 1, 2",
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

#[test]
fn hir_reads_indexes_on_generated_columns_like_legacy() {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_generated_columns(true))
        .build();
    let [legacy, hir] = connections(&db);
    for statement in [
        "CREATE TABLE t(a INTEGER, b INTEGER, c AS (a * 2) VIRTUAL, d AS (b + c) VIRTUAL, \
         z AS ('Z') VIRTUAL)",
        "CREATE INDEX t_c ON t(c)",
        "CREATE INDEX t_d ON t(d, a)",
        "CREATE INDEX t_z ON t(z)",
        "INSERT INTO t(a, b) VALUES (1, 10), (2, 20), (3, NULL), (NULL, 5)",
    ] {
        legacy.execute(statement).unwrap();
    }
    for sql in [
        "SELECT a FROM t WHERE z = 'Z' ORDER BY a",
        "SELECT a, c FROM t WHERE c > 1 ORDER BY c DESC",
        "SELECT d, a FROM t WHERE d = 24",
        "SELECT min(c), max(c) FROM t",
        "SELECT c, count(*) FROM t GROUP BY c ORDER BY c",
        // `d` is indexed and reads `c`, so `c` must be computable here too.
        "SELECT x.a, y.a FROM t x JOIN t y ON y.c = x.a * 2 ORDER BY 1, 2",
        "SELECT a FROM t WHERE c IN (2, 6) ORDER BY a",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "rows differ for {sql}"
        );
    }
}

#[test]
fn hir_grouping_over_an_expression_index_matches_legacy() {
    let db = setup();
    let [legacy, hir] = connections(&db);
    legacy
        .execute("CREATE INDEX items_value_mod ON items((value % 20))")
        .unwrap();
    for sql in [
        // The grouped rows keep `value`, which the index does not hold.
        "SELECT (value % 20) AS m, count(*) FROM items GROUP BY m ORDER BY m",
        "SELECT value % 20, row_number() OVER (ORDER BY value % 20, id) FROM items ORDER BY 2",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "rows differ for {sql}"
        );
    }
}

#[test]
fn hir_unqualified_sqlite_master_is_the_main_schema() {
    let db = setup();
    let [legacy, hir] = connections(&db);
    for conn in [&legacy, &hir] {
        conn.execute("CREATE TEMP TABLE temp_only(x)").unwrap();
    }
    for sql in [
        "SELECT count(*) FROM sqlite_master WHERE name = 'temp_only'",
        "SELECT count(*) FROM sqlite_schema WHERE name = 'items'",
        "SELECT count(*) FROM temp.sqlite_master WHERE name = 'temp_only'",
        "SELECT count(*) FROM sqlite_temp_master WHERE name = 'temp_only'",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "rows differ for {sql}"
        );
    }
}

#[test]
fn hir_materialized_view_shows_the_transactions_own_changes() {
    let db = TempDatabase::builder().with_views(true).build();
    let [legacy, hir] = connections(&db);
    legacy
        .execute("CREATE TABLE t(a INTEGER, b INTEGER)")
        .unwrap();
    legacy
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    legacy
        .execute("CREATE MATERIALIZED VIEW v AS SELECT * FROM t WHERE b > 15")
        .unwrap();
    let mut seen = Vec::new();
    for conn in [&legacy, &hir] {
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES (3, 30)").unwrap();
        let rows = limbo_exec_rows(conn, "SELECT * FROM v ORDER BY a");
        conn.execute("ROLLBACK").unwrap();
        seen.push(rows);
    }
    assert_eq!(seen[1], seen[0]);
    assert_eq!(seen[1].len(), 2, "the uncommitted row is visible");
}

#[test]
fn hir_displays_arrays_passed_through_expressions_like_legacy() {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_custom_types(true))
        .build();
    let [legacy, hir] = connections(&db);
    legacy
        .execute("CREATE TABLE a(id INTEGER PRIMARY KEY, vals integer[]) STRICT")
        .unwrap();
    legacy
        .execute("INSERT INTO a VALUES (1, '{4,5,6}'), (2, NULL)")
        .unwrap();
    for sql in [
        "SELECT COALESCE(vals, vals), IFNULL(vals, ARRAY[1]) FROM a ORDER BY id",
        "SELECT CASE WHEN id = 1 THEN vals END, vals || vals FROM a ORDER BY id",
        "SELECT COALESCE(NULL, ARRAY[3, 4]), MAX(ARRAY[1, 2], ARRAY[1, 2, 3])",
        "SELECT NULLIF(vals, NULL) FROM a ORDER BY vals",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "rows differ for {sql}"
        );
    }
}

#[test]
fn hir_real_column_comparisons_keep_large_integers_exact() {
    let db = setup();
    let [legacy, hir] = connections(&db);
    for statement in [
        "CREATE TABLE reals(f REAL)",
        "CREATE INDEX reals_f ON reals(f)",
        "INSERT INTO reals VALUES (-2701729208700874036)",
    ] {
        legacy.execute(statement).unwrap();
    }
    // A REAL column against an integer compares as NUMERIC, so the integer
    // is not rounded to the stored float.
    for sql in [
        "SELECT count(*) FROM reals WHERE f = -2701729208700874036",
        "SELECT count(*) FROM reals WHERE f = -2701729208700874240",
        "SELECT count(*) FROM reals NOT INDEXED WHERE f = -2701729208700874036",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "rows differ for {sql}"
        );
    }
}

#[test]
fn hir_reads_an_aliased_table_whose_partial_index_names_the_table() {
    let db = TempDatabase::new_empty();
    let [legacy, hir] = connections(&db);
    for statement in [
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, amount INTEGER, active INTEGER)",
        // The index WHERE names the table itself, not the statement's alias.
        "CREATE INDEX orders_active ON orders(amount) WHERE orders.active = 1",
        "INSERT INTO orders VALUES (1, 10, 1), (2, 20, 0), (3, 30, 1)",
    ] {
        legacy.execute(statement).unwrap();
    }
    for sql in [
        "SELECT * FROM orders AS o WHERE o.amount > 5 ORDER BY o.id",
        "SELECT o.amount FROM orders o WHERE o.active = 1 AND o.amount > 0",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "rows differ for {sql}"
        );
    }
}

#[test]
fn hir_errors_match_legacy() {
    let db = setup();
    let [legacy, hir] = connections(&db);
    for sql in [
        "SELECT * FROM main.nosuch",
        "SELECT * FROM nosuch",
        "SELECT nosuch FROM items",
    ] {
        let legacy_error = legacy
            .prepare(sql)
            .err()
            .expect("legacy rejects")
            .to_string();
        let hir_error = hir.prepare(sql).err().expect("HIR rejects").to_string();
        assert_eq!(hir_error, legacy_error, "{sql}");
    }
    // Legacy says "no such table: x" here; HIR names the column as SQLite
    // does.
    let error = hir
        .prepare("SELECT x.y FROM items")
        .err()
        .expect("HIR rejects")
        .to_string();
    assert_eq!(error, "Parse error: no such column: x.y");
}

#[test]
fn hir_min_and_max_stop_at_the_first_value_an_index_returns() {
    let db = TempDatabase::new_empty();
    let hir = db.connect_limbo();
    hir.set_experimental_hir(true);
    for statement in [
        "CREATE TABLE t(x, y)",
        "CREATE INDEX t_x ON t(x)",
        "INSERT INTO t VALUES (NULL, 1), (3, 2), (1, 3), (NULL, 4), (7, 0), (5, 9)",
        "CREATE TABLE u(a, b)",
        "CREATE INDEX u_a ON u(a NULLS LAST)",
        "CREATE INDEX u_b ON u(b DESC NULLS FIRST)",
        "INSERT INTO u VALUES (NULL, NULL), (2, 8), (9, 1), (4, NULL)",
        "CREATE TABLE empty(x)",
        "CREATE INDEX empty_x ON empty(x)",
    ] {
        hir.execute(statement).unwrap();
    }
    for (sql, expected) in [
        (
            "SELECT min(x) FROM t",
            vec![vec![rusqlite::types::Value::Integer(1)]],
        ),
        (
            "SELECT max(x) FROM t",
            vec![vec![rusqlite::types::Value::Integer(7)]],
        ),
        (
            "SELECT max(x) FROM t WHERE y > 1",
            vec![vec![rusqlite::types::Value::Integer(5)]],
        ),
        (
            "SELECT min(x) FROM t WHERE x > 2",
            vec![vec![rusqlite::types::Value::Integer(3)]],
        ),
        // NULLs sort at the end of these indexes, so the first non-NULL
        // value is not the first row the scan reads. Legacy returns NULL
        // for both MAX queries.
        (
            "SELECT min(a) FROM u",
            vec![vec![rusqlite::types::Value::Integer(2)]],
        ),
        (
            "SELECT max(a) FROM u",
            vec![vec![rusqlite::types::Value::Integer(9)]],
        ),
        (
            "SELECT max(b) FROM u",
            vec![vec![rusqlite::types::Value::Integer(8)]],
        ),
        (
            "SELECT min(b) FROM u",
            vec![vec![rusqlite::types::Value::Integer(1)]],
        ),
        (
            "SELECT max(x) FROM empty",
            vec![vec![rusqlite::types::Value::Null]],
        ),
    ] {
        assert_eq!(limbo_exec_rows(&hir, sql), expected, "{sql}");
    }

    // The scan ends at the first non-NULL value instead of reading every
    // row of the index.
    let opcodes = limbo_exec_rows(&hir, "EXPLAIN SELECT max(x) FROM t")
        .into_iter()
        .filter_map(|row| match row.get(1) {
            Some(rusqlite::types::Value::Text(opcode)) => Some(opcode.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let step = opcodes
        .iter()
        .position(|opcode| opcode == "AggStep")
        .expect("MAX steps its aggregate");
    assert_eq!(opcodes[step + 1], "Goto", "{opcodes:?}");
}

#[test]
fn hir_seek_keys_from_a_null_row_skip_the_seek() {
    let db = TempDatabase::new_empty();
    let hir = db.connect_limbo();
    hir.set_experimental_hir(true);
    for statement in [
        "CREATE TABLE a(id INTEGER PRIMARY KEY)",
        "CREATE TABLE b(id INTEGER PRIMARY KEY, aid)",
        "CREATE TABLE c(x INTEGER, y)",
        "CREATE INDEX cx ON c(x)",
        "INSERT INTO a VALUES (1), (2)",
        "INSERT INTO b VALUES (10, 1)",
        "INSERT INTO c VALUES (NULL, 'null'), (10, 'ten')",
        "INSERT INTO c WITH RECURSIVE n(i) AS (SELECT 100 UNION ALL SELECT i + 1 FROM n WHERE i < 3000) \
         SELECT i, 'filler' FROM n",
        "ANALYZE",
    ] {
        hir.execute(statement).unwrap();
    }
    // b.id is a rowid, but b's NULL row for a = 2 makes it NULL. The range
    // seek on c must not start from that NULL key; legacy does and returns
    // every row of c above it.
    let sql = "SELECT a.id, b.id, c.y FROM a \
               LEFT JOIN b ON b.id = a.id + 9 \
               LEFT JOIN c ON c.x > b.id AND c.x < 102";
    let plan = limbo_exec_rows(&hir, &format!("EXPLAIN QUERY PLAN {sql}"));
    assert!(
        plan.iter().any(|row| matches!(
            row.get(3),
            Some(rusqlite::types::Value::Text(detail)) if detail.starts_with("SEARCH c USING INDEX cx")
        )),
        "{plan:?}"
    );
    let filler = || rusqlite::types::Value::Text("filler".to_string());
    assert_eq!(
        limbo_exec_rows(&hir, sql),
        vec![
            vec![
                rusqlite::types::Value::Integer(1),
                rusqlite::types::Value::Integer(10),
                filler(),
            ],
            vec![
                rusqlite::types::Value::Integer(1),
                rusqlite::types::Value::Integer(10),
                filler(),
            ],
            vec![
                rusqlite::types::Value::Integer(2),
                rusqlite::types::Value::Null,
                rusqlite::types::Value::Null,
            ],
        ]
    );
}

#[test]
fn hir_correlated_subqueries_seek_an_automatic_index() {
    let db = TempDatabase::new_empty();
    let hir = db.connect_limbo();
    hir.set_experimental_hir(true);
    for statement in [
        "CREATE TABLE o(id INTEGER PRIMARY KEY, k INT, v INT)",
        "CREATE TABLE i(id INTEGER PRIMARY KEY, k INT, v INT)",
        "INSERT INTO o WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 500) \
         SELECT x, x % 200, x FROM n",
        "INSERT INTO i WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 500) \
         SELECT x, x % 300, x FROM n",
    ] {
        hir.execute(statement).unwrap();
    }
    // Like SQLite and legacy, the subquery runs once per outer row, so it
    // builds an index on i(k) once and seeks it on every run instead of
    // scanning i each time.
    for (sql, expected) in [
        (
            "SELECT count(*) FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k = o.k)",
            500,
        ),
        (
            "SELECT count(*) FROM o WHERE o.v < (SELECT avg(i.v) FROM i WHERE i.k = o.k)",
            200,
        ),
    ] {
        let plan = limbo_exec_rows(&hir, &format!("EXPLAIN QUERY PLAN {sql}"));
        assert!(
            plan.iter().any(|row| matches!(
                row.get(3),
                Some(rusqlite::types::Value::Text(detail))
                    if detail.starts_with("SEARCH i USING COVERING INDEX ephemeral")
            )),
            "{sql}: {plan:?}"
        );
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            vec![vec![rusqlite::types::Value::Integer(expected)]],
            "{sql}"
        );
    }
}

#[test]
fn hir_count_star_counts_without_a_loop() {
    let db = setup();
    let hir = db.connect_limbo();
    hir.set_experimental_hir(true);
    let opcodes = |sql: &str| {
        limbo_exec_rows(&hir, &format!("EXPLAIN {sql}"))
            .into_iter()
            .filter_map(|row| match row.get(1) {
                Some(rusqlite::types::Value::Text(opcode)) => Some(opcode.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    let program = opcodes("SELECT count(*) FROM items");
    assert!(
        program.iter().any(|opcode| opcode == "Count"),
        "{program:?}"
    );
    assert!(
        program.iter().any(|opcode| opcode == "Transaction"),
        "{program:?}"
    );
    assert!(
        !program.iter().any(|opcode| opcode == "Next"),
        "{program:?}"
    );
    // A WHERE term needs the loop.
    let filtered = opcodes("SELECT count(*) FROM items WHERE value > 10");
    assert!(
        !filtered.iter().any(|opcode| opcode == "Count"),
        "{filtered:?}"
    );

    let count = |sql: &str| limbo_exec_rows(&hir, sql);
    let six = vec![vec![rusqlite::types::Value::Integer(6)]];
    assert_eq!(count("SELECT count(*) FROM items"), six);
    assert_eq!(count("SELECT DISTINCT count(*) FROM items"), six);
    assert_eq!(
        count("SELECT count(*) FROM items LIMIT 0"),
        Vec::<Vec<rusqlite::types::Value>>::new()
    );
    assert_eq!(
        count("SELECT count(*) FROM tags"),
        vec![vec![rusqlite::types::Value::Integer(4)]]
    );
}

#[test]
fn hir_between_on_a_column_seeks_an_index_range() {
    let db = setup();
    let [legacy, hir] = connections(&db);
    // Like legacy and SQLite, each bound of BETWEEN can drive the seek.
    let sql = "SELECT id FROM items WHERE value BETWEEN 15 AND 35 ORDER BY id";
    let plan = limbo_exec_rows(&hir, &format!("EXPLAIN QUERY PLAN {sql}"));
    assert!(
        plan.iter().any(|row| matches!(
            row.get(3),
            Some(rusqlite::types::Value::Text(detail))
                if detail == "SEARCH items USING COVERING INDEX items_value (value>=? AND value<=?)"
        )),
        "{plan:?}"
    );
    for sql in [
        sql,
        "SELECT id FROM items WHERE value NOT BETWEEN 15 AND 35 ORDER BY id",
        "SELECT id FROM items WHERE value + 0 BETWEEN 15 AND 35 ORDER BY id",
        "SELECT id FROM items WHERE 20 BETWEEN value AND value + 10 ORDER BY id",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "{sql}"
        );
    }
}

#[test]
fn hir_conditions_jump_on_comparisons_like_legacy() {
    let db = setup();
    let [legacy, hir] = connections(&db);
    let opcodes = |sql: &str| {
        limbo_exec_rows(&hir, &format!("EXPLAIN {sql}"))
            .into_iter()
            .filter_map(|row| match row.get(1) {
                Some(rusqlite::types::Value::Text(opcode)) => Some(opcode.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    // `extra = 'b'` is one jump on the comparison, and its constant is
    // loaded once, after the program's Transaction.
    let program = opcodes("SELECT id FROM items WHERE extra = 'b'");
    assert!(program.iter().any(|opcode| opcode == "Ne"), "{program:?}");
    assert!(
        !program
            .iter()
            .any(|opcode| opcode == "ZeroOrNull" || opcode == "IfNot"),
        "{program:?}"
    );
    let transaction = program
        .iter()
        .position(|opcode| opcode == "Transaction")
        .expect("program reads a table");
    let constant = program
        .iter()
        .rposition(|opcode| opcode == "String8")
        .expect("the constant is loaded");
    assert!(constant > transaction, "{program:?}");

    for sql in [
        "SELECT id FROM items WHERE value = 20 ORDER BY id",
        "SELECT id FROM items WHERE value != 20 ORDER BY id",
        "SELECT id FROM items WHERE value < 30 OR extra = 'c' ORDER BY id",
        "SELECT id FROM items WHERE NOT (value >= 30) ORDER BY id",
        "SELECT id FROM items WHERE value IS 20 ORDER BY id",
        "SELECT id FROM items WHERE value IS NOT 20 ORDER BY id",
        "SELECT id FROM items WHERE value IS NOT NULL AND value <= 30 ORDER BY id",
        "SELECT id FROM items WHERE (value > 15) IS NULL ORDER BY id",
        "SELECT id, CASE WHEN value > 25 THEN 'big' ELSE 'small' END FROM items ORDER BY id",
        "SELECT count(*) FROM items i JOIN tags t ON t.item_id = i.id AND t.tag = 'x'",
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            limbo_exec_rows(&legacy, sql),
            "{sql}"
        );
    }
}
