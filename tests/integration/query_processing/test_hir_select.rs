//! SELECT prepared through semantic HIR: the rows it returns are checked
//! against SQLite reading the same database file, and its plans against what
//! the planner is meant to choose.

use crate::common::{limbo_exec_rows, limbo_exec_rows_fallible, sqlite_exec_rows, TempDatabase};

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

/// A connection that prepares statements through HIR, and SQLite on the
/// same database file as the oracle for the rows and column names a query
/// returns.
fn connections(db: &TempDatabase) -> (Oracle, std::sync::Arc<turso_core::Connection>) {
    let hir = db.connect_limbo();
    let sqlite = Oracle {
        hir: hir.clone(),
        path: db.path.clone(),
        pragmas: Vec::new(),
    };
    (sqlite, hir)
}

/// SQLite reading the database file the HIR connection writes. SQLite does
/// not follow the WAL Turso keeps appending to, so before each query Turso
/// checkpoints everything into the database file and SQLite opens that file
/// afresh, read-only, without touching the WAL.
struct Oracle {
    hir: std::sync::Arc<turso_core::Connection>,
    path: std::path::PathBuf,
    /// Connection settings every fresh connection gets.
    pragmas: Vec<(&'static str, bool)>,
}

impl Oracle {
    fn open(&self) -> rusqlite::Connection {
        self.hir.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let conn = rusqlite::Connection::open_with_flags(
            format!("file:{}?immutable=1", self.path.display()),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )
        .unwrap();
        for (name, value) in &self.pragmas {
            conn.pragma_update(None, name, value).unwrap();
        }
        conn
    }

    fn set_pragma(&mut self, name: &'static str, value: bool) {
        self.pragmas.retain(|(existing, _)| *existing != name);
        self.pragmas.push((name, value));
    }

    fn rows(&self, sql: &str) -> Vec<Vec<rusqlite::types::Value>> {
        sqlite_exec_rows(&self.open(), sql)
    }

    /// The name and declared type of each column of `sql`.
    fn column_metadata(&self, sql: &str) -> Vec<(String, Option<String>)> {
        let conn = self.open();
        let stmt = conn.prepare(sql).unwrap();
        stmt.columns()
            .iter()
            .map(|column| {
                (
                    column.name().to_string(),
                    column.decl_type().map(|ty| ty.to_string()),
                )
            })
            .collect()
    }

    fn column_names(&self, sql: &str) -> Vec<String> {
        self.column_metadata(sql)
            .into_iter()
            .map(|(name, _)| name)
            .collect()
    }
}

/// HIR returns the rows SQLite returns for `sql`, in the same order when the
/// query orders them; without ORDER BY each engine may scan a different
/// index, so the order is not compared.
fn assert_same_rows(hir: &std::sync::Arc<turso_core::Connection>, sqlite: &Oracle, sql: &str) {
    let (hir_rows, sqlite_rows) = (limbo_exec_rows(hir, sql), sqlite.rows(sql));
    if sql.contains("ORDER BY") {
        assert_eq!(hir_rows, sqlite_rows, "rows differ for {sql}");
    } else {
        assert_eq!(
            sorted(hir_rows),
            sorted(sqlite_rows),
            "rows differ for {sql}"
        );
    }
}

/// HIR returns the rows SQLite returns for `sql`, in any order: the query's
/// ORDER BY has ties, which each engine may break differently.
fn assert_same_rows_any_order(
    hir: &std::sync::Arc<turso_core::Connection>,
    sqlite: &Oracle,
    sql: &str,
) {
    assert_eq!(
        sorted(limbo_exec_rows(hir, sql)),
        sorted(sqlite.rows(sql)),
        "rows differ for {sql}"
    );
}

fn sorted(mut rows: Vec<Vec<rusqlite::types::Value>>) -> Vec<Vec<rusqlite::types::Value>> {
    rows.sort_by_key(|row| format!("{row:?}"));
    rows
}

/// The EXPLAIN QUERY PLAN rows the planner is meant to choose for `sql`.
fn expected_plan(sql: &str) -> Vec<String> {
    match EXPECTED_PLANS
        .iter()
        .find(|(candidate, _)| *candidate == sql)
    {
        Some((_, plan)) => plan.iter().map(|row| row.to_string()).collect(),
        None => {
            eprintln!("CAPTURE_MISSING\t{sql}");
            crate::common::last_captured_plan(sql)
                .unwrap_or_else(|| panic!("no expected plan recorded for {sql}"))
        }
    }
}

#[test]
fn hir_select_matches_sqlite_rows_and_columns() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
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
        // A subquery column reading a rowid is named after the identifier
        // as written, not the rowid alias.
        "SELECT d.rowid FROM (SELECT items.rowid FROM items) AS d WHERE d.rowid < 3 ORDER BY 1",
        "SELECT * FROM (SELECT items.rowid FROM items) WHERE rowid < 3 ORDER BY 1",
        "SELECT * FROM (SELECT i.value FROM items i WHERE i.id < 3) ORDER BY 1",
        // A compound ORDER BY term matches an output expression of any arm.
        "SELECT a.id FROM items a UNION ALL SELECT b.value FROM items b ORDER BY b.value",
        "SELECT value + 1 FROM items UNION SELECT id FROM tags, items ORDER BY value + 1",
        "WITH s AS (SELECT id FROM items) SELECT s.id, x.id FROM s, s AS x WHERE s.id = x.id",
    ] {
        assert_same_rows(&hir, &sqlite, sql);
        assert_eq!(
            column_metadata(&hir, sql)
                .into_iter()
                .map(|(name, _, declared_type)| (name, declared_type))
                .collect::<Vec<_>>(),
            sqlite.column_metadata(sql),
            "columns differ for {sql}"
        );
    }
}

#[test]
fn hir_window_select_matches_sqlite_rows() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for sql in [
        "SELECT id, sum(value) OVER (PARTITION BY extra ORDER BY id) FROM items",
        // Each engine numbers the rows in its own scan order, so only the
        // numbers themselves are compared.
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
        if sql.contains("OVER ()") {
            let column = |rows: Vec<Vec<rusqlite::types::Value>>, n: usize| {
                sorted(rows.into_iter().map(|row| vec![row[n].clone()]).collect())
            };
            let (hir_rows, sqlite_rows) = (limbo_exec_rows(&hir, sql), sqlite.rows(sql));
            assert_eq!(column(hir_rows.clone(), 0), column(sqlite_rows.clone(), 0), "{sql}");
            assert_eq!(column(hir_rows, 1), column(sqlite_rows, 1), "{sql}");
        } else {
            assert_same_rows(&hir, &sqlite, sql);
        }
        assert_eq!(column_names(&hir, sql), sqlite.column_names(sql), "{sql}");
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
fn hir_expression_subqueries_match_sqlite_rows() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
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
        "SELECT id FROM items ORDER BY id LIMIT (SELECT 2)",
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
        assert_same_rows(&hir, &sqlite, sql);
    }
}

#[test]
fn hir_views_expand_like_sqlite() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE VIEW cheap AS SELECT id, value FROM items WHERE value < 25",
        "CREATE VIEW renamed(n, label) AS SELECT value * 10, upper(extra) FROM items",
        "CREATE VIEW nested AS SELECT n FROM renamed WHERE n > 100",
        "CREATE VIEW loop_a AS SELECT * FROM items",
    ] {
        hir.execute(statement).unwrap();
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
        assert_same_rows(&hir, &sqlite, sql);
        assert_eq!(column_names(&hir, sql), sqlite.column_names(sql), "{sql}");
    }

    hir.execute("DROP VIEW loop_a").unwrap();
    hir.execute("CREATE VIEW loop_b AS SELECT 1 AS one")
        .unwrap();
    hir.execute("CREATE VIEW loop_a AS SELECT * FROM loop_b")
        .unwrap();
    hir.execute("DROP VIEW loop_b").unwrap();
    hir.execute("CREATE VIEW loop_b AS SELECT * FROM loop_a")
        .unwrap();
    let error = hir
        .prepare("SELECT * FROM loop_a")
        .expect_err("circular view fails");
    assert!(error.to_string().contains("circularly defined"), "{error}");
}

#[test]
fn hir_right_and_full_joins_match_sqlite_rows() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
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
        assert_same_rows(&hir, &sqlite, sql);
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
        "SELECT a.id, b.id, t.tag FROM items a FULL JOIN items b ON a.id = b.id \
         FULL JOIN tags t ON t.item_id = b.id ORDER BY 1, 2, 3",
        "SELECT a.id, a.value, b.value FROM items a FULL JOIN items b USING (id) ORDER BY 1, 2, 3",
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
}

#[test]
fn hir_reads_indexes_on_generated_columns() {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new())
        .build();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE t(a INTEGER, b INTEGER, c AS (a * 2) VIRTUAL, d AS (b + c) VIRTUAL, \
         z AS ('Z') VIRTUAL)",
        "CREATE INDEX t_c ON t(c)",
        "CREATE INDEX t_d ON t(d, a)",
        "CREATE INDEX t_z ON t(z)",
        "INSERT INTO t(a, b) VALUES (1, 10), (2, 20), (3, NULL), (NULL, 5)",
    ] {
        hir.execute(statement).unwrap();
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
        assert_same_rows(&hir, &sqlite, sql);
    }
}

#[test]
fn hir_grouping_over_an_expression_index_matches_sqlite() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    hir.execute("CREATE INDEX items_value_mod ON items((value % 20))")
        .unwrap();
    for sql in [
        // The grouped rows keep `value`, which the index does not hold.
        "SELECT (value % 20) AS m, count(*) FROM items GROUP BY m ORDER BY m",
        "SELECT value % 20, row_number() OVER (ORDER BY value % 20, id) FROM items ORDER BY 2",
    ] {
        assert_same_rows(&hir, &sqlite, sql);
    }
}

#[test]
fn hir_unqualified_sqlite_master_is_the_main_schema() {
    let db = setup();
    let (_, hir) = connections(&db);
    hir.execute("CREATE TEMP TABLE temp_only(x)").unwrap();
    for (sql, count) in [
        (
            "SELECT count(*) FROM sqlite_master WHERE name = 'temp_only'",
            0,
        ),
        ("SELECT count(*) FROM sqlite_schema WHERE name = 'items'", 1),
        (
            "SELECT count(*) FROM temp.sqlite_master WHERE name = 'temp_only'",
            1,
        ),
        (
            "SELECT count(*) FROM sqlite_temp_master WHERE name = 'temp_only'",
            1,
        ),
    ] {
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            [[rusqlite::types::Value::Integer(count)]],
            "rows differ for {sql}"
        );
    }
}

#[test]
fn hir_materialized_view_shows_the_transactions_own_changes() {
    let db = TempDatabase::builder().with_views(true).build();
    let (_, hir) = connections(&db);
    hir.execute("CREATE TABLE t(a INTEGER, b INTEGER)").unwrap();
    hir.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    hir.execute("CREATE MATERIALIZED VIEW v AS SELECT * FROM t WHERE b > 15")
        .unwrap();
    hir.execute("BEGIN").unwrap();
    hir.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    let rows = limbo_exec_rows(&hir, "SELECT * FROM v ORDER BY a");
    hir.execute("ROLLBACK").unwrap();
    assert_eq!(rows.len(), 2, "the uncommitted row is visible");
}

#[test]
fn hir_displays_arrays_passed_through_expressions() {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_custom_types(true))
        .build();
    let hir = db.connect_limbo();
    hir.execute("CREATE TABLE a(id INTEGER PRIMARY KEY, vals integer[]) STRICT")
        .unwrap();
    hir.execute("INSERT INTO a VALUES (1, '{4,5,6}'), (2, NULL)")
        .unwrap();
    for sql in [
        "SELECT COALESCE(vals, vals), IFNULL(vals, ARRAY[1]) FROM a ORDER BY id",
        "SELECT CASE WHEN id = 1 THEN vals END, vals || vals FROM a ORDER BY id",
        "SELECT COALESCE(NULL, ARRAY[3, 4]), MAX(ARRAY[1, 2], ARRAY[1, 2, 3])",
        "SELECT NULLIF(vals, NULL) FROM a ORDER BY vals",
    ] {
        eprintln!("CAPTURE_ROWS\t{sql}\t{:?}", limbo_exec_rows(&hir, sql));
    }
    // SQLite has no arrays, so the rows are pinned.
    let text = |value: &str| rusqlite::types::Value::Text(value.to_string());
    for (sql, expected) in [
        (
            "SELECT COALESCE(vals, vals), IFNULL(vals, ARRAY[1]) FROM a ORDER BY id",
            vec![
                vec![text("{4,5,6}"), text("{4,5,6}")],
                vec![rusqlite::types::Value::Null, text("{1}")],
            ],
        ),
        (
            "SELECT CASE WHEN id = 1 THEN vals END, vals || vals FROM a ORDER BY id",
            vec![
                vec![text("{4,5,6}"), text("{4,5,6,4,5,6}")],
                vec![rusqlite::types::Value::Null, rusqlite::types::Value::Null],
            ],
        ),
        (
            "SELECT COALESCE(NULL, ARRAY[3, 4]), MAX(ARRAY[1, 2], ARRAY[1, 2, 3])",
            vec![vec![text("{3,4}"), text("{1,2,3}")]],
        ),
        (
            "SELECT NULLIF(vals, NULL) FROM a ORDER BY vals",
            vec![vec![rusqlite::types::Value::Null], vec![text("{4,5,6}")]],
        ),
    ] {
        assert_eq!(limbo_exec_rows(&hir, sql), expected, "{sql}");
    }
}

#[test]
fn hir_aggregate_subquery_in_a_grouped_query_keeps_the_group_keys() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for sql in [
        "SELECT extra FROM items GROUP BY extra HAVING 1 <> (SELECT count(*) FROM tags) ORDER BY 1",
        "SELECT extra, sum(value) FROM items GROUP BY extra \
         HAVING max(value) <> (SELECT count(*) FROM tags GROUP BY tag ORDER BY 1 LIMIT 1) ORDER BY 1",
        "SELECT (SELECT count(*) FROM tags GROUP BY tag ORDER BY 1 LIMIT 1), extra FROM items \
         GROUP BY extra ORDER BY 2",
    ] {
        assert_same_rows(&hir, &sqlite, sql);
    }
}

#[test]
fn hir_subquery_outputs_stay_arrays_for_the_outer_query() {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_custom_types(true))
        .build();
    let hir = db.connect_limbo();
    hir.execute("CREATE TABLE a(id INTEGER PRIMARY KEY, vals integer[]) STRICT")
        .unwrap();
    hir.execute("INSERT INTO a VALUES (1, '{4,5,6}'), (2, '{7}')")
        .unwrap();
    let integer = rusqlite::types::Value::Integer;
    let text = |value: &str| rusqlite::types::Value::Text(value.to_string());
    for (sql, expected) in [
        (
            "SELECT array_element(e, 2), e FROM (SELECT ARRAY[10, 20, 30] AS e)",
            vec![vec![integer(20), text("{10,20,30}")]],
        ),
        (
            "SELECT array_element(e, 3) FROM (SELECT column1 AS e FROM (VALUES (ARRAY[10, 20, 30])))",
            vec![vec![integer(30)]],
        ),
        (
            "WITH c AS (SELECT ARRAY[10, 20, 30] AS e) SELECT array_element(e, 1) FROM c",
            vec![vec![integer(10)]],
        ),
        (
            "SELECT array_element(e, 1) FROM (SELECT ARRAY[id * 10] AS e FROM a ORDER BY id DESC)",
            vec![vec![integer(20)], vec![integer(10)]],
        ),
        (
            "SELECT array_element(vals, 1), vals FROM (SELECT vals FROM a WHERE id = 1)",
            vec![vec![integer(4), text("{4,5,6}")]],
        ),
    ] {
        assert_eq!(limbo_exec_rows(&hir, sql), expected, "{sql}");
    }
}

#[test]
fn hir_real_column_comparisons_keep_large_integers_exact() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE reals(f REAL)",
        "CREATE INDEX reals_f ON reals(f)",
        "INSERT INTO reals VALUES (-2701729208700874036)",
    ] {
        hir.execute(statement).unwrap();
    }
    // A REAL column against an integer compares as NUMERIC, so the integer
    // is not rounded to the stored float.
    for sql in [
        "SELECT count(*) FROM reals WHERE f = -2701729208700874036",
        "SELECT count(*) FROM reals WHERE f = -2701729208700874240",
        "SELECT count(*) FROM reals NOT INDEXED WHERE f = -2701729208700874036",
    ] {
        assert_same_rows(&hir, &sqlite, sql);
    }
}

#[test]
fn hir_reads_an_aliased_table_whose_partial_index_names_the_table() {
    let db = TempDatabase::new_empty();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, amount INTEGER, active INTEGER)",
        // The index WHERE names the table itself, not the statement's alias.
        "CREATE INDEX orders_active ON orders(amount) WHERE orders.active = 1",
        "INSERT INTO orders VALUES (1, 10, 1), (2, 20, 0), (3, 30, 1)",
    ] {
        hir.execute(statement).unwrap();
    }
    for sql in [
        "SELECT * FROM orders AS o WHERE o.amount > 5 ORDER BY o.id",
        "SELECT o.amount FROM orders o WHERE o.active = 1 AND o.amount > 0",
    ] {
        assert_same_rows(&hir, &sqlite, sql);
    }
}

#[test]
fn hir_errors_name_what_is_wrong() {
    let db = setup();
    let (_, hir) = connections(&db);
    for (sql, message) in [
        (
            "SELECT * FROM main.nosuch",
            "Parse error: no such table: main.nosuch",
        ),
        ("SELECT * FROM nosuch", "Parse error: no such table: nosuch"),
        (
            "SELECT nosuch FROM items",
            "Parse error: no such column: nosuch",
        ),
        (
            "SELECT * FROM items WHERE extra MATCH 'a'",
            "Parse error: unable to use function MATCH in the requested context",
        ),
        (
            "SELECT * FROM items WHERE (extra, id) MATCH 'a'",
            "Parse error: unable to use function MATCH in the requested context",
        ),
        (
            "SELECT * FROM items WHERE NOT (lower(extra) MATCH 'a')",
            "Parse error: unable to use function MATCH in the requested context",
        ),
        (
            "SELECT * FROM items i JOIN tags t ON i.id = t.item_id AND t.tag MATCH 'x'",
            "Parse error: unable to use function MATCH in the requested context",
        ),
        (
            "SELECT (SELECT 1 FROM tags WHERE tag MATCH 'x') FROM items",
            "Parse error: unable to use function MATCH in the requested context",
        ),
        (
            "DELETE FROM items WHERE extra MATCH 'a'",
            "Parse error: unable to use function MATCH in the requested context",
        ),
        (
            "UPDATE items SET value = 1 WHERE extra MATCH 'a' OR value = 2",
            "Parse error: unable to use function MATCH in the requested context",
        ),
    ] {
        let hir_error = hir.prepare(sql).expect_err("HIR rejects").to_string();
        assert_eq!(hir_error, message, "{sql}");
    }
    // A qualifier that names no table or CTE of the query is reported as
    // the missing table, as main's planner did and as the lateral-errors
    // sqltests expect (SQLite reports the column instead).
    let error = hir
        .prepare("SELECT x.y FROM items")
        .expect_err("HIR rejects")
        .to_string();
    assert_eq!(error, "Parse error: no such table: x");
}

#[test]
fn hir_min_and_max_stop_at_the_first_value_an_index_returns() {
    let db = TempDatabase::new_empty();
    let hir = db.connect_limbo();
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
fn hir_correlated_subqueries_match_rows() {
    let db = TempDatabase::new_empty();
    let hir = db.connect_limbo();
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
    // Like legacy, the EXISTS becomes a semi-join, which reaches i(k)
    // through an automatic index, and the average is computed once per key
    // of i in a grouped FROM subquery.
    assert_eq!(
        query_plan(
            &hir,
            "SELECT count(*) FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k = o.k)"
        ),
        [
            "SCAN o",
            "SEARCH i USING COVERING INDEX ephemeral_i_s1 (k=?)"
        ]
    );
    assert_eq!(
        query_plan(
            &hir,
            "SELECT count(*) FROM o WHERE o.v < (SELECT avg(i.v) FROM i WHERE i.k = o.k)"
        ),
        [
            "SCAN o",
            "SEARCH scalar_subquery_1 USING INDEX ephemeral_subquery_s2 (correlation_key_0=?)",
            "SCAN i",
            "USE SORTER FOR GROUP BY",
        ]
    );
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
    let (sqlite, hir) = connections(&db);
    // Like legacy and SQLite, each bound of BETWEEN can drive the seek.
    assert_eq!(
        query_plan(&hir, "SELECT id FROM items WHERE value BETWEEN 15 AND 35"),
        ["SEARCH items USING COVERING INDEX items_value (value>=? AND value<=?)"]
    );
    // Without statistics the planner counts a million rows, and a scan in
    // rowid order then costs less than the seek plus a sort of its rows.
    let sql = "SELECT id FROM items WHERE value BETWEEN 15 AND 35 ORDER BY id";
    assert_eq!(query_plan(&hir, sql), ["SCAN items"]);
    for sql in [
        sql,
        "SELECT id FROM items WHERE value NOT BETWEEN 15 AND 35 ORDER BY id",
        "SELECT id FROM items WHERE value + 0 BETWEEN 15 AND 35 ORDER BY id",
        "SELECT id FROM items WHERE 20 BETWEEN value AND value + 10 ORDER BY id",
    ] {
        assert_same_rows(&hir, &sqlite, sql);
    }
}

#[test]
fn hir_conditions_jump_on_comparisons() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
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
        assert_same_rows(&hir, &sqlite, sql);
    }
}

#[test]
fn hir_not_in_checks_the_subquery_for_null_once() {
    let db = TempDatabase::new_empty();
    let hir = db.connect_limbo();
    for statement in [
        "CREATE TABLE a(x)",
        "CREATE TABLE b(y)",
        "CREATE TABLE c(z)",
        "CREATE TABLE e(w)",
        "INSERT INTO a VALUES (1), (2), (NULL), (4)",
        "INSERT INTO b VALUES (2), (NULL), (3)",
        "INSERT INTO c VALUES (2), (3)",
    ] {
        hir.execute(statement).unwrap();
    }
    use rusqlite::types::Value::{Integer, Null};
    // A value the subquery lacks is NULL when the subquery holds a NULL,
    // false or true otherwise; a NULL value is NULL unless it is empty.
    for (sql, expected) in [
        (
            "SELECT x IN (SELECT y FROM b), x NOT IN (SELECT y FROM b) FROM a ORDER BY rowid",
            vec![
                vec![Null, Null],
                vec![Integer(1), Integer(0)],
                vec![Null, Null],
                vec![Null, Null],
            ],
        ),
        (
            "SELECT x IN (SELECT z FROM c), x NOT IN (SELECT z FROM c) FROM a ORDER BY rowid",
            vec![
                vec![Integer(0), Integer(1)],
                vec![Integer(1), Integer(0)],
                vec![Null, Null],
                vec![Integer(0), Integer(1)],
            ],
        ),
        (
            "SELECT x IN (SELECT w FROM e), x NOT IN (SELECT w FROM e) FROM a ORDER BY rowid",
            vec![vec![Integer(0), Integer(1)]; 4],
        ),
    ] {
        assert_eq!(limbo_exec_rows(&hir, sql), expected, "{sql}");
    }

    // The NULL check reads one index entry instead of looping over the
    // subquery's rows for every outer row.
    let program = limbo_exec_rows(
        &hir,
        "EXPLAIN SELECT count(*) FROM a WHERE x NOT IN (SELECT z FROM c)",
    )
    .into_iter()
    .filter_map(|row| match row.get(1) {
        Some(rusqlite::types::Value::Text(opcode)) => Some(opcode.clone()),
        _ => None,
    })
    .collect::<Vec<_>>();
    let nexts = program.iter().filter(|opcode| *opcode == "Next").count();
    // One Next for the scan of a and one for building the subquery index.
    assert_eq!(nexts, 2, "{program:?}");
}

#[test]
fn hir_lifts_conjuncts_every_or_branch_shares() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    // Like legacy, `value = 20` is in both branches, so it is a WHERE term
    // of its own and can seek the index on value.
    let sql = "SELECT id FROM items \
               WHERE (value = 20 AND extra = 'a') OR (value = 20 AND extra = 'b') \
               ORDER BY id";
    let plan = limbo_exec_rows(&hir, &format!("EXPLAIN QUERY PLAN {sql}"));
    assert!(
        plan.iter().any(|row| matches!(
            row.get(3),
            Some(rusqlite::types::Value::Text(detail))
                if detail.starts_with("SEARCH items USING INDEX items_value (value=?)")
        )),
        "{plan:?}"
    );
    for sql in [
        sql,
        "SELECT id FROM items WHERE value = 20 OR (value = 20 AND extra = 'b') ORDER BY id",
        "SELECT id FROM items WHERE (value > 10 AND extra = 'a') OR (extra = 'a' AND id = 3) \
         OR (extra = 'a' AND value IS NULL) ORDER BY id",
        "SELECT id FROM items WHERE (value = 20 AND extra = 'a') OR (value = 30) ORDER BY id",
    ] {
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// A correlated IN that runs like EXISTS and a correlated aggregate whose
/// result is cached per key return SQLite's rows. Legacy's subquery rewrites
/// return wrong rows for all three queries.
#[test]
fn hir_correlated_in_probe_and_cached_aggregates_match_sqlite() {
    const SCHEMA: &str = "
        CREATE TABLE o(id INTEGER PRIMARY KEY, k, a, c TEXT COLLATE NOCASE);
        CREATE TABLE i(k INT, a, x INT, n INT);
        CREATE TABLE j(k INT, m INT);
        CREATE TABLE p(id INTEGER PRIMARY KEY, ok);
        INSERT INTO o VALUES (1,1,1,'a'),(2,1.0,NULL,'A'),(3,'1','2','b'),(4,2,2.0,NULL),
            (5,2,5,'B'),(6,NULL,1,'a'),(7,1,7,'a'),(8,x'31',8,'c');
        INSERT INTO i VALUES (1,1,10,1),(1,NULL,20,2),(2,2,30,3),(2,'2',40,4),(3,7,50,5),
            (NULL,1,60,6);
        INSERT INTO j VALUES (1,1),(2,3),(3,5);
        INSERT INTO p VALUES (1,1),(2,2),(3,NULL);
    ";
    let db = TempDatabase::new_empty();
    let hir = db.connect_limbo();
    for statement in SCHEMA.split(';').filter(|sql| !sql.trim().is_empty()) {
        hir.execute(statement).unwrap();
    }
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch(SCHEMA).unwrap();
    for sql in [
        // The IN runs like EXISTS over a join inside the subquery.
        "SELECT id FROM o WHERE a IN (SELECT i.a FROM i JOIN j ON j.k = i.k \
         WHERE i.k = o.k AND j.m > 0) ORDER BY id",
        // The subquery reads an outer aggregate, so it is not cached.
        "SELECT quote(k), (SELECT count(*) + max(o.a) FROM i WHERE i.k = o.k) \
         FROM o GROUP BY k ORDER BY 1",
        // The cached aggregate joins two tables of its own.
        "SELECT id, (SELECT count(*) FROM i, p WHERE i.k = p.ok AND p.id = o.k) \
         FROM o ORDER BY id",
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
}

/// An IN subquery that reads the outer row only inside its own FROM
/// subquery still depends on that row, so it cannot drive a seek on it.
#[test]
fn hir_in_subquery_reading_the_outer_row_through_a_from_subquery_matches_sqlite() {
    const SCHEMA: &str = "
        CREATE TABLE o(id INTEGER PRIMARY KEY, k INT);
        CREATE TABLE i(k INT, amount INT);
        INSERT INTO o VALUES (1,1),(2,1),(3,2),(4,3),(5,NULL),(6,2);
        INSERT INTO i VALUES (1,10),(1,20),(2,6),(2,3),(3,4),(NULL,5);
    ";
    let db = TempDatabase::new_empty();
    let hir = db.connect_limbo();
    for statement in SCHEMA.split(';').filter(|sql| !sql.trim().is_empty()) {
        hir.execute(statement).unwrap();
    }
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch(SCHEMA).unwrap();
    for sql in [
        "SELECT id FROM o WHERE id IN \
         (SELECT amount FROM (SELECT amount FROM i WHERE i.k = o.k)) ORDER BY id",
        "SELECT id FROM o WHERE id IN \
         (SELECT amount FROM (SELECT DISTINCT amount FROM i WHERE i.k = o.k)) ORDER BY id",
        "SELECT id FROM o WHERE id NOT IN \
         (SELECT amount FROM (SELECT amount FROM i WHERE i.k = o.k)) ORDER BY id",
    ] {
        let mut stmt = sqlite.prepare(sql).unwrap();
        let expected: Vec<Vec<rusqlite::types::Value>> = stmt
            .query_map([], |row| Ok(vec![row.get(0)?]))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            limbo_exec_rows(&hir, sql),
            expected,
            "rows differ for {sql}"
        );
    }
}

/// Like legacy, an equality join on columns no index covers may build a hash
/// table from the earlier table and scan the later one, probing it; a LEFT
/// join also scans the build rows nothing matched. A build table read with
/// a constraint is stored first, as a build input. A FULL JOIN never
/// hashes. The tables are large enough that a debug build's hash table
/// spills to disk, so the lookups that wait for spilled rows run too, and
/// every row still matches SQLite. The row counts come from ANALYZE: without
/// them the planner counts a million rows per table, and a build input of
/// that size is too large to store.
#[test]
fn hir_equality_joins_without_an_index_use_a_hash_table() {
    const SCHEMA: &str = "
        CREATE TABLE a(id INTEGER PRIMARY KEY, k INT, pad TEXT);
        CREATE TABLE b(id INTEGER PRIMARY KEY, k INT, pad TEXT, v INT);
        WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 1500)
            INSERT INTO a SELECT i, CASE WHEN i % 13 = 0 THEN NULL ELSE i % 40 END,
                substr('pppppppppppppppppppppppppppppppppppppppp', 1, i % 40) FROM s;
        WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 1200)
            INSERT INTO b SELECT i, CASE WHEN i % 11 = 0 THEN NULL ELSE i % 50 END,
                substr('qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq', 1, i % 50), i % 17 FROM s;
    ";
    let db = TempDatabase::new_empty();
    let (_, hir) = connections(&db);
    for statement in SCHEMA.split(';').filter(|sql| !sql.trim().is_empty()) {
        hir.execute(statement).unwrap();
    }
    hir.execute("ANALYZE").unwrap();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch(SCHEMA).unwrap();
    // A LEFT JOIN row without a match, including one whose lookup waited
    // for a partition on disk, reads NULL. Like legacy, a subquery that
    // runs again probes the same hash table again.
    for (sql, hashes) in [
        (
            "SELECT count(*), sum(a.id), sum(b.id), max(a.pad || b.pad) FROM a JOIN b ON b.k = a.k",
            true,
        ),
        (
            "SELECT a.k, count(*), sum(b.v) FROM a JOIN b ON b.k = a.k GROUP BY a.k ORDER BY a.k",
            true,
        ),
        (
            "SELECT a.id, b.id FROM a JOIN b ON b.k = a.k WHERE a.id < 30 ORDER BY a.id, b.id",
            true,
        ),
        (
            "SELECT count(*), count(b.id), sum(b.v) FROM a LEFT JOIN b ON b.k = a.k",
            true,
        ),
        (
            "SELECT count(*), count(b.id) FROM a LEFT JOIN b ON b.k = a.k AND b.v <> 3",
            true,
        ),
        (
            "SELECT count(*), sum(a.id) FROM a LEFT JOIN b ON b.k = a.k WHERE b.id IS NULL",
            true,
        ),
        (
            "SELECT a.id, b.id FROM a LEFT JOIN b ON b.k = a.k + 45 WHERE a.id < 20 \
             ORDER BY a.id, b.id",
            true,
        ),
        // b.v has 17 values, so most lookups land in a part of the hash
        // table with no rows, which never waits for the disk.
        (
            "SELECT count(*), count(b.id) FROM a LEFT JOIN b ON b.v = a.k + 100",
            true,
        ),
        (
            "SELECT count(*), count(a.id), count(b.id), sum(b.v) FROM a FULL JOIN b ON b.k = a.k",
            false,
        ),
        (
            "SELECT count(*), count(a.id), count(b.id) FROM a FULL JOIN b ON b.k = a.k \
             WHERE a.id IS NULL OR b.v > 10",
            false,
        ),
        (
            "SELECT sum((SELECT count(b.id) FROM a a2 JOIN b ON b.k = a2.k WHERE a2.id = a.id)) \
             FROM a WHERE a.id < 300",
            true,
        ),
        (
            "SELECT count(*), sum(s.n) FROM a, (SELECT a2.id AS i, b.id AS n FROM a a2 \
             JOIN b ON b.k = a2.k WHERE a2.id < 40) s WHERE a.id < 4 AND s.i = a.id + 3",
            true,
        ),
    ] {
        let opcodes = limbo_exec_rows(&hir, &format!("EXPLAIN {sql}"))
            .into_iter()
            .filter_map(|row| match row.get(1) {
                Some(rusqlite::types::Value::Text(opcode)) => Some(opcode.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        for opcode in ["HashBuild", "HashProbe", "HashGraceInit"] {
            assert_eq!(
                opcodes.iter().any(|candidate| candidate == opcode),
                hashes,
                "{opcode} for {sql}: {opcodes:?}"
            );
        }
        // The same tables are hashed and scanned as in legacy's plan, where
        // legacy hashes too.
        if hashes {
            assert_eq!(
                plan_without_ids(query_plan(&hir, sql)),
                plan_without_ids(expected_plan(sql)),
                "{sql}"
            );
        }
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
}

/// Like legacy and SQLite, an ORDER BY whose terms are all GROUP BY keys
/// reuses the GROUP BY sort, comparing those keys first, instead of sorting
/// the groups again.
#[test]
fn hir_order_by_group_keys_sorts_once() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    let sorters = |conn: &std::sync::Arc<turso_core::Connection>, sql: &str| {
        limbo_exec_rows(conn, &format!("EXPLAIN {sql}"))
            .into_iter()
            .filter(|row| {
                matches!(row.get(1), Some(rusqlite::types::Value::Text(opcode)) if opcode == "SorterOpen")
            })
            .count()
    };
    for (sql, expected) in [
        (
            "SELECT extra, count(*) FROM items GROUP BY extra ORDER BY extra",
            1,
        ),
        (
            "SELECT extra, count(*) FROM items GROUP BY extra ORDER BY 1 DESC",
            1,
        ),
        (
            "SELECT extra, id, count(*) FROM items GROUP BY extra, id ORDER BY extra",
            1,
        ),
        (
            "SELECT extra, id, count(*) FROM items GROUP BY extra, id ORDER BY id",
            1,
        ),
        (
            "SELECT extra, count(*) FROM items GROUP BY extra ORDER BY count(*)",
            2,
        ),
    ] {
        assert_eq!(sorters(&hir, sql), expected, "{sql}");
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// The detail column of each EXPLAIN QUERY PLAN row.
/// `plan` without the ids that end the names of temporary indexes and
/// subqueries, which differ between HIR (`_s3`, `_1`) and legacy (`_t2`).
fn plan_without_ids(plan: Vec<String>) -> Vec<String> {
    plan.into_iter()
        .map(|line| {
            line.split(' ')
                .map(|word| match word.rsplit_once('_') {
                    Some((name, id))
                        if (word.starts_with("ephemeral_") || word.contains("subquery_"))
                            && id.trim_start_matches(['s', 't']).parse::<u32>().is_ok() =>
                    {
                        name
                    }
                    _ => word,
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

fn query_plan(conn: &std::sync::Arc<turso_core::Connection>, sql: &str) -> Vec<String> {
    limbo_exec_rows(conn, &format!("EXPLAIN QUERY PLAN {sql}"))
        .into_iter()
        .map(|row| match row.last() {
            Some(rusqlite::types::Value::Text(detail)) => detail.clone(),
            other => panic!("EXPLAIN QUERY PLAN detail is not text: {other:?}"),
        })
        .collect()
}

/// Like legacy's `compute_order_target`, a grouped query aims for its GROUP
/// BY order even when it also has an ORDER BY, so an index on the keys
/// replaces the GROUP BY sorter. When the ORDER BY names only GROUP BY keys,
/// the index order in ORDER BY's directions replaces both sorters.
#[test]
fn hir_group_by_uses_an_index_when_the_query_also_has_order_by() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    hir.execute("CREATE INDEX items_extra_value ON items(extra, value)")
        .unwrap();
    for sql in [
        "SELECT extra, count(*) AS n FROM items GROUP BY extra ORDER BY n DESC",
        "SELECT extra, value, count(*) FROM items GROUP BY extra, value ORDER BY extra, value",
        "SELECT extra, value, count(*) FROM items GROUP BY extra, value \
         ORDER BY extra DESC, value DESC",
        "SELECT extra, value, count(*) FROM items GROUP BY value, extra ORDER BY extra",
        "SELECT extra, value, count(*) FROM items GROUP BY extra, value ORDER BY value",
        "SELECT extra, value, count(*) FROM items GROUP BY extra, value \
         ORDER BY extra DESC NULLS FIRST, value",
        "SELECT extra, max(value) FROM items GROUP BY extra ORDER BY extra",
    ] {
        let plan = query_plan(&hir, sql);
        assert_eq!(plan, expected_plan(sql), "{sql}");
        assert_same_rows(&hir, &sqlite, sql);
    }
    assert_eq!(
        query_plan(
            &hir,
            "SELECT extra, value, count(*) FROM items GROUP BY extra, value ORDER BY extra, value"
        ),
        ["SCAN items USING COVERING INDEX items_extra_value"]
    );
}

/// Like legacy, `x IS NULL` seeks an index on x, also through an expression
/// index, but not on a table a LEFT JOIN may NULL-extend, where `IS NULL`
/// is true on the added row. HIR also seeks for the `x ISNULL` spelling,
/// which SQLite treats the same and legacy does not recognize. With
/// ORDER BY id and no statistics, a scan in rowid order costs less than a
/// seek plus a sort of the million rows the planner counts.
#[test]
fn hir_is_null_seeks_an_index() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE INDEX items_extra_id ON items(extra, value)",
        "CREATE INDEX items_lower_extra ON items(lower(extra))",
        "INSERT INTO items VALUES (7, NULL, NULL), (8, 70, NULL)",
    ] {
        hir.execute(statement).unwrap();
    }
    for sql in [
        "SELECT id FROM items WHERE extra IS NULL ORDER BY id",
        "SELECT id FROM items WHERE extra IS NULL AND value IS NULL",
        "SELECT id FROM items WHERE extra IS NULL AND value > 10 ORDER BY id",
        "SELECT id FROM items WHERE lower(extra) IS NULL ORDER BY id",
        "SELECT tags.tag, items.id FROM tags LEFT JOIN items ON items.id = tags.item_id \
         WHERE items.extra IS NULL ORDER BY 1",
        "SELECT tags.tag, items.id FROM tags LEFT JOIN items \
         ON items.extra IS NULL AND items.id = tags.item_id ORDER BY 1",
    ] {
        assert_eq!(query_plan(&hir, sql), expected_plan(sql), "{sql}");
        assert_same_rows(&hir, &sqlite, sql);
    }
    assert_eq!(
        query_plan(&hir, "SELECT id FROM items WHERE extra IS NULL"),
        ["SEARCH items USING COVERING INDEX items_extra_id (extra=?)"]
    );
    assert_eq!(
        query_plan(&hir, "SELECT id FROM items WHERE extra ISNULL"),
        ["SEARCH items USING COVERING INDEX items_extra_id (extra=?)"]
    );
    let sql = "SELECT id FROM items WHERE extra ISNULL ORDER BY id";
    assert_same_rows(&hir, &sqlite, sql);
}

/// When one seek per OR branch applies every condition of the OR term, the
/// rows it finds are exactly the rows the term accepts, so like legacy the
/// term is not checked again and a subquery in it fills its list once. A
/// branch with a condition its seek does not apply keeps the check.
#[test]
fn hir_or_union_runs_each_in_subquery_once() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE v(id INTEGER PRIMARY KEY, a INT, b INT, c INT)",
        "CREATE INDEX va ON v(a)",
        "CREATE INDEX vb ON v(b)",
        "CREATE TABLE ra(x INT)",
        "CREATE TABLE rb(x INT)",
        "INSERT INTO v VALUES (1,1,0,5),(2,0,3,6),(3,5,5,7),(4,1,3,8),(5,NULL,NULL,9),(6,2,NULL,1)",
        "INSERT INTO ra VALUES (1),(2),(NULL)",
        "INSERT INTO rb VALUES (3)",
    ] {
        hir.execute(statement).unwrap();
    }
    let opens = |sql: &str, table: &str| {
        limbo_exec_rows(&hir, &format!("EXPLAIN {sql}"))
            .into_iter()
            .filter(|row| {
                matches!(row.get(1), Some(rusqlite::types::Value::Text(opcode)) if opcode == "OpenRead")
                    && matches!(row.last(), Some(rusqlite::types::Value::Text(comment))
                        if comment.starts_with(&format!("table={table},")))
            })
            .count()
    };
    let sql =
        "SELECT id FROM v WHERE a IN (SELECT x FROM ra) OR b IN (SELECT x FROM rb) ORDER BY id";
    assert!(
        query_plan(&hir, sql).contains(&"MULTI-INDEX OR v (va, vb)".to_string()),
        "{sql}"
    );
    assert_eq!(opens(sql, "ra"), 1, "{sql}");
    assert_eq!(opens(sql, "rb"), 1, "{sql}");
    for sql in [
        sql,
        "SELECT id FROM v WHERE (a = 1 AND c > 6) OR b = 3 ORDER BY id",
        "SELECT id FROM v WHERE a = 1 OR b = 3 ORDER BY id",
        "SELECT id FROM v WHERE a = '1' OR b = 3.0 ORDER BY id",
        "SELECT id FROM v WHERE a IN (1, 2) OR b > 4 ORDER BY id",
    ] {
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// Like legacy, a FROM subquery or CTE whose columns earlier loops constrain
/// is stored once and read through a temporary index on those columns,
/// instead of being scanned again for every outer row, also inside a
/// correlated subquery. The right side of a RIGHT JOIN is still scanned,
/// since a second pass looks for its unmatched rows.
#[test]
fn hir_subqueries_and_ctes_seek_a_temporary_index() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INT, day TEXT, total REAL)",
        "CREATE TABLE points(v INT)",
        "INSERT INTO customers VALUES (1,'a'),(2,'b'),(3,'c')",
        "INSERT INTO orders VALUES (10,1,'2024-02-01',50),(11,1,'2023-01-01',2000),\
         (12,2,'2024-05-01',1500),(13,9,'2024-06-01',7),(14,NULL,'2024-07-01',3)",
        "INSERT INTO points VALUES (1),(2),(4),(5),(6),(9),(2),(NULL)",
    ] {
        hir.execute(statement).unwrap();
    }
    for (sql, search) in [
        (
            "WITH recent AS (SELECT id, customer_id FROM orders WHERE day >= '2024-01-01') \
             SELECT c.id FROM customers c \
             WHERE EXISTS (SELECT 1 FROM recent r WHERE r.customer_id = c.id) ORDER BY 1",
            Some("SEARCH r USING INDEX ephemeral_subquery_"),
        ),
        (
            "SELECT c.name, s.total FROM customers c JOIN (SELECT customer_id, sum(total) AS total \
             FROM orders GROUP BY customer_id) AS s ON s.customer_id = c.id \
             WHERE s.total > 500 ORDER BY 1",
            Some("SEARCH s USING INDEX ephemeral_subquery_"),
        ),
        (
            "WITH sorted AS (SELECT DISTINCT v FROM points ORDER BY v) \
             SELECT p1.v, (SELECT max(p2.v) FROM sorted p2 WHERE p2.v < p1.v) \
             FROM sorted p1 ORDER BY 1",
            Some("SEARCH p2 USING INDEX ephemeral_subquery_"),
        ),
        (
            "SELECT c.name, s.n FROM customers c LEFT JOIN (SELECT customer_id, count(*) AS n \
             FROM orders GROUP BY customer_id) AS s ON s.customer_id = c.id ORDER BY 1",
            None,
        ),
        (
            "SELECT c.name, s.n FROM customers c RIGHT JOIN (SELECT customer_id, count(*) AS n \
             FROM orders GROUP BY customer_id) AS s ON s.customer_id = c.id ORDER BY 1, 2",
            None,
        ),
    ] {
        if let Some(search) = search {
            let plan = query_plan(&hir, sql);
            assert!(
                plan.iter().any(|row| row.starts_with(search)),
                "{sql}: {plan:?}"
            );
            assert!(
                expected_plan(sql)
                    .iter()
                    .any(|row| row.starts_with(search)),
                "{sql}"
            );
        }
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// Like legacy, a FROM subquery or CTE returns its rows in the order of its
/// own ORDER BY, of its GROUP BY keys, or of the one subquery it passes
/// through, so an outer ORDER BY that asks for that order needs no sort.
#[test]
fn hir_ordered_subquery_skips_the_outer_sort() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for sql in [
        "WITH o AS (SELECT value, extra FROM items ORDER BY value) SELECT * FROM o ORDER BY value",
        "WITH g AS (SELECT extra, count(*) AS n FROM items GROUP BY extra) \
         SELECT * FROM g ORDER BY extra",
        "WITH a AS (SELECT value, extra FROM items ORDER BY value), b AS (SELECT value, extra FROM a) \
         SELECT * FROM b ORDER BY value",
        "SELECT * FROM (SELECT value, extra FROM items ORDER BY value DESC NULLS FIRST) \
         ORDER BY value DESC NULLS FIRST",
        // These still sort: the direction or the terms differ.
        "WITH o AS (SELECT value, extra FROM items ORDER BY value) SELECT * FROM o ORDER BY value DESC",
        "WITH o AS (SELECT value, extra FROM items ORDER BY value) SELECT * FROM o ORDER BY value, extra",
    ] {
        assert_eq!(query_plan(&hir, sql), expected_plan(sql), "{sql}");
        // `value` has ties.
        assert_same_rows_any_order(&hir, &sqlite, sql);
    }
    assert_eq!(
        query_plan(
            &hir,
            "WITH o AS (SELECT value, extra FROM items ORDER BY extra) SELECT * FROM o ORDER BY extra",
        ),
        ["SCAN o", "SCAN items", "USE SORTER FOR ORDER BY"]
    );
    // Like legacy, a CTE stored once is read backwards for the opposite
    // order. Legacy prints the CTE body before its scan, so compare sorts.
    let sorts = |plan: Vec<String>| {
        plan.iter()
            .filter(|row| row.as_str() == "USE SORTER FOR ORDER BY")
            .count()
    };
    for (sql, expected_sorts) in [
        (
            "WITH o AS MATERIALIZED (SELECT value, extra FROM items ORDER BY extra DESC, value DESC) \
             SELECT * FROM o ORDER BY extra, value",
            1,
        ),
        (
            "WITH o AS MATERIALIZED (SELECT value, extra FROM items ORDER BY extra, value DESC) \
             SELECT * FROM o ORDER BY extra DESC, value",
            1,
        ),
        (
            "WITH g AS MATERIALIZED (SELECT extra, sum(value) AS s FROM items GROUP BY extra) \
             SELECT * FROM g ORDER BY extra DESC",
            0,
        ),
        (
            "WITH o AS MATERIALIZED (SELECT value, extra FROM items ORDER BY extra, value DESC) \
             SELECT * FROM o ORDER BY extra, value",
            2,
        ),
    ] {
        assert_eq!(sorts(query_plan(&hir, sql)), expected_sorts, "{sql}");
        assert_eq!(sorts(expected_plan(sql)), expected_sorts, "{sql}");
        assert_same_rows(&hir, &sqlite, sql);
    }
    // The index gives the CTE its order. Legacy forgets the CTE's ORDER BY
    // once the index satisfies it, and sorts again; HIR keeps it.
    let sql =
        "WITH o AS (SELECT value, extra FROM items ORDER BY value LIMIT 3) SELECT * FROM o ORDER BY value";
    assert_eq!(
        query_plan(&hir, sql),
        ["SCAN o", "SCAN items USING INDEX items_value"]
    );
    assert_same_rows(&hir, &sqlite, sql);
}

/// Like legacy, whose window source subquery orders its rows by the window's
/// PARTITION BY and ORDER BY, rows that the loops already return in that
/// order, through an index or an ordered CTE, need no window sort.
#[test]
fn hir_window_uses_the_loop_order() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    hir.execute("CREATE INDEX items_extra_value ON items(extra, value)")
        .unwrap();
    let sorters = |sql: &str| {
        limbo_exec_rows(&hir, &format!("EXPLAIN {sql}"))
            .into_iter()
            .filter(|row| {
                matches!(row.get(1), Some(rusqlite::types::Value::Text(opcode)) if opcode == "SorterOpen")
            })
            .count()
    };
    // Legacy shows its window source subquery as a scan of its own and its
    // window sort as an ORDER BY sort; HIR shows neither, and the sorters
    // are counted in the bytecode instead. None of these queries has an
    // ORDER BY of its own.
    let plan = |rows: Vec<String>| {
        rows.into_iter()
            .filter(|row| {
                !row.starts_with("SCAN window_subquery_") && row != "USE SORTER FOR ORDER BY"
            })
            .collect::<Vec<_>>()
    };
    for (sql, expected_sorters) in [
        (
            "SELECT id, row_number() OVER (ORDER BY value DESC) FROM items",
            0,
        ),
        (
            "SELECT id, extra, sum(value) OVER (PARTITION BY extra ORDER BY value) FROM items",
            0,
        ),
        (
            "SELECT id, rank() OVER (PARTITION BY extra ORDER BY value DESC) FROM items",
            1,
        ),
        (
            "WITH o AS (SELECT id, value FROM items ORDER BY value) \
             SELECT id, sum(value) OVER (ORDER BY value) FROM o",
            0,
        ),
        (
            "WITH o AS (SELECT id, extra FROM items NOT INDEXED ORDER BY extra) \
             SELECT id, count(*) OVER (ORDER BY extra) FROM o",
            1,
        ),
        (
            "SELECT id, row_number() OVER (ORDER BY value) FROM items LIMIT 3",
            0,
        ),
    ] {
        assert_eq!(sorters(sql), expected_sorters, "{sql}");
        assert_eq!(
            plan(query_plan(&hir, sql)),
            plan(expected_plan(sql)),
            "{sql}"
        );
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// Like SQLite, a FULL JOIN runs as nested loops plus a pass over the right
/// table's rows nothing matched, which lists after the loops under a
/// `RIGHT-JOIN` row. It is never a hash join: a hash join does not record
/// which right rows matched. The right table is scanned, not read through
/// a temporary index, since the unmatched pass reads the table again. A
/// WHERE term that is false on the rows the join adds on the right, here
/// `a.id = 1` on a table left of the join, makes it a LEFT JOIN, which
/// hashes.
#[test]
fn hir_full_join_runs_as_nested_loops_with_an_unmatched_pass() {
    const SCHEMA: &str = "
        CREATE TABLE l(id INTEGER PRIMARY KEY, k TEXT, v INT);
        CREATE TABLE r(id INTEGER PRIMARY KEY, k TEXT, w INT);
        INSERT INTO l VALUES (1,'a',1),(2,'b',2),(3,NULL,3),(4,'d',4),(5,'a',5);
        INSERT INTO r VALUES (10,'a',10),(11,'a',11),(12,'c',12),(13,NULL,13),(14,'d',14);
    ";
    let db = TempDatabase::new_empty();
    let (_, hir) = connections(&db);
    for statement in SCHEMA.split(';').filter(|sql| !sql.trim().is_empty()) {
        hir.execute(statement).unwrap();
    }
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch(SCHEMA).unwrap();
    let sql = "SELECT l.id, r.id FROM l FULL JOIN r ON l.k = r.k ORDER BY 1, 2";
    let plan = query_plan(&hir, sql);
    assert_eq!(plan, expected_plan(sql));
    assert_eq!(
        plan,
        [
            "SCAN l",
            "SCAN r LEFT-JOIN",
            "RIGHT-JOIN r",
            "SCAN r",
            "USE SORTER FOR ORDER BY",
        ]
    );
    let nested_sql = "SELECT a.id, l.id, r.id FROM l AS a, l FULL JOIN r ON l.k = r.k \
                      WHERE a.id = 1 ORDER BY 1, 2, 3";
    let plan = query_plan(&hir, nested_sql);
    assert_eq!(plan, expected_plan(nested_sql));
    assert_eq!(
        plan,
        [
            "SEARCH a USING INTEGER PRIMARY KEY (rowid=?)",
            "HASH JOIN r",
            "SCAN l",
            "USE SORTER FOR ORDER BY",
        ]
    );
    for sql in [
        sql,
        nested_sql,
        "SELECT l.id, r.id FROM l FULL JOIN r ON l.k = r.k AND r.w > 10 ORDER BY 1, 2",
        "SELECT l.id, r.id FROM l FULL JOIN r ON l.k = r.k WHERE l.v IS NULL OR r.w > 10 \
         ORDER BY 1, 2",
        "SELECT count(*), count(l.id), count(r.id) FROM l FULL JOIN r ON l.k = r.k",
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
}

/// Like legacy, HIR plans a correlated subquery for the number of times the
/// outer plan runs it, estimates a grouped query by its groups rather than
/// the rows it reads, and treats a lone MIN or MAX over a FROM subquery or
/// CTE like one over a table, seeking its bound instead of walking past it.
#[test]
fn hir_subquery_estimates_drive_the_plan() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE emp(id INTEGER PRIMARY KEY, dept TEXT, salary INT)",
        "CREATE TABLE cust(id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE ord(id INTEGER PRIMARY KEY, cust_id INT, total INT)",
        "CREATE INDEX ord_cust ON ord(cust_id)",
        "CREATE TABLE prod(id INTEGER PRIMARY KEY, price INT, name TEXT)",
        "CREATE INDEX prod_price ON prod(price)",
        "INSERT INTO emp VALUES (1,'a',10),(2,'a',30),(3,'b',5),(4,'b',NULL),(5,NULL,7)",
        "INSERT INTO cust VALUES (1,'x'),(2,'y'),(3,'z')",
        "INSERT INTO ord VALUES (1,1,10),(2,1,20),(3,2,5),(4,9,1)",
        "INSERT INTO prod VALUES (1,5,'a'),(2,50,'b'),(3,500,'c'),(4,NULL,'d'),(5,99,'e')",
    ] {
        hir.execute(statement).unwrap();
    }
    for (sql, rows) in [
        (
            "WITH da AS (SELECT dept, avg(salary) AS a FROM emp GROUP BY dept) \
             SELECT e.id FROM emp e WHERE e.salary > \
             (SELECT da.a FROM da WHERE da.dept = e.dept) ORDER BY 1",
            ["SEARCH da USING INDEX ephemeral_subquery_"].as_slice(),
        ),
        (
            "WITH t AS (SELECT cust_id, sum(total) AS s FROM ord GROUP BY cust_id) \
             SELECT c.name, t.s FROM cust c JOIN t ON t.cust_id = c.id \
             WHERE t.s > (SELECT avg(s) FROM t) ORDER BY 1",
            ["SCAN cust AS c", "SEARCH t USING INDEX ephemeral_subquery_"].as_slice(),
        ),
        (
            // Walking the CTE's price order from the end would step over
            // every row above the bound for each outer row, so MAX seeks a
            // temporary index on the bound instead.
            "WITH p(price) AS MATERIALIZED (SELECT price FROM prod) \
             SELECT p1.price, (SELECT max(p2.price) FROM p p2 WHERE p2.price < p1.price) \
             FROM p p1 WHERE p1.price < 100",
            ["SEARCH p2 USING INDEX ephemeral_subquery_"].as_slice(),
        ),
    ] {
        let plan = query_plan(&hir, sql);
        for row in rows {
            assert!(
                plan.iter().any(|detail| detail.starts_with(row)),
                "plan lacks {row}: {sql}: {plan:?}"
            );
        }
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// Like legacy, a correlated subquery opens its table cursors on its first
/// run only, instead of once for every outer row, and gives the same rows.
#[test]
fn hir_correlated_subqueries_open_their_cursors_once() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for sql in [
        // A term that reads only the outer table keeps NOT EXISTS a subquery.
        "SELECT count(*) FROM items i WHERE NOT EXISTS \
         (SELECT 1 FROM tags t WHERE t.item_id = i.id AND i.value = 20)",
        "SELECT i.id, (SELECT count(*) FROM tags t WHERE t.item_id >= i.id AND t.item_id <= i.id) \
         FROM items i ORDER BY 1",
    ] {
        let program = limbo_exec_rows(&hir, &format!("EXPLAIN {sql}"));
        let opcode = |row: &Vec<rusqlite::types::Value>| match row.get(1) {
            Some(rusqlite::types::Value::Text(opcode)) => opcode.clone(),
            _ => String::new(),
        };
        let open = program
            .iter()
            .position(|row| {
                opcode(row) == "OpenRead"
                    && matches!(row.get(7), Some(rusqlite::types::Value::Text(comment)) if comment.contains("tags"))
            })
            .expect("the subquery opens tags");
        assert_eq!(opcode(&program[open - 1]), "Once", "{sql}: {program:?}");
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// Like legacy, a grouped row keeps only the columns read after grouping:
/// the GROUP BY keys and aggregate arguments, not the columns only a join
/// or WHERE reads. The rows still match legacy.
#[test]
fn hir_grouped_rows_keep_only_columns_read_after_grouping() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    let sorter_columns = |conn: &std::sync::Arc<turso_core::Connection>, sql: &str| {
        limbo_exec_rows(conn, &format!("EXPLAIN {sql}"))
            .into_iter()
            .find_map(|row| match (row.get(1), row.get(3)) {
                (
                    Some(rusqlite::types::Value::Text(opcode)),
                    Some(rusqlite::types::Value::Integer(columns)),
                ) if opcode == "SorterOpen" => Some(*columns),
                _ => None,
            })
            .expect("the GROUP BY sorter is opened")
    };
    for (sql, columns) in [
        (
            "SELECT t.tag, count(*), sum(i.value) FROM items i JOIN tags t ON t.item_id = i.id \
             WHERE i.extra <> 'z' GROUP BY t.tag",
            2,
        ),
        (
            "SELECT i.extra, max(i.value) FROM items i WHERE i.id > 1 GROUP BY i.extra \
             HAVING min(i.id) > 0",
            3,
        ),
    ] {
        assert_eq!(sorter_columns(&hir, sql), columns, "{sql}");
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// Like legacy, an operand with the same value on every row is computed
/// once: a literal before the loops, anything else, such as `upper('x')`,
/// the first time it is reached. A constant that fails, such as
/// `abs(-9223372036854775808)`, fails only where SQLite's does: not in a
/// CASE branch, COALESCE argument or WHERE term no row reaches.
#[test]
fn hir_constant_operands_are_computed_once() {
    const SCHEMA: &str = "
        CREATE TABLE t(a INT, b TEXT);
        INSERT INTO t VALUES (1, 'p'), (2, 'q'), (3, NULL);
    ";
    let db = TempDatabase::new_empty();
    let hir = db.connect_limbo();
    for statement in SCHEMA.split(';').filter(|sql| !sql.trim().is_empty()) {
        hir.execute(statement).unwrap();
    }
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch(SCHEMA).unwrap();
    for sql in [
        "SELECT max(upper('x') || b), sum(a * 2 + length('abc')) FROM t",
        "SELECT a + 1, substr('abcdef', 1, a) || b FROM t ORDER BY a",
        "SELECT CASE WHEN a > 5 THEN a + abs(-9223372036854775808) ELSE a END FROM t ORDER BY a",
        "SELECT coalesce(a, 1 + abs(-9223372036854775808)) FROM t ORDER BY a",
        "SELECT a FROM t WHERE a > 5 AND a + abs(-9223372036854775808) > 0",
        "SELECT a + abs(-9223372036854775808) FROM t WHERE a > 5",
        "SELECT a + abs(-9223372036854775808) FROM t",
    ] {
        let mut stmt = sqlite.prepare(sql).unwrap();
        let columns = stmt.column_count();
        let expected: Result<Vec<Vec<rusqlite::types::Value>>, _> = stmt
            .query_map([], |row| {
                (0..columns).map(|column| row.get(column)).collect()
            })
            .unwrap()
            .collect();
        match expected {
            Ok(expected) => assert_eq!(limbo_exec_rows(&hir, sql), expected, "{sql}"),
            Err(_) => assert!(
                limbo_exec_rows_fallible(&db, &hir, sql).is_err(),
                "{sql} fails in SQLite"
            ),
        }
    }

    let program = limbo_exec_rows(&hir, "EXPLAIN SELECT max(upper('x') || b) FROM t");
    let opcode = |row: &Vec<rusqlite::types::Value>| match row.get(1) {
        Some(rusqlite::types::Value::Text(opcode)) => opcode.clone(),
        _ => String::new(),
    };
    let upper = program
        .iter()
        .position(|row| {
            opcode(row) == "Function"
                && matches!(row.get(5), Some(rusqlite::types::Value::Text(name)) if name == "upper")
        })
        .expect("upper is called");
    // A Once before the call jumps past it after the first row.
    let integer = |row: &Vec<rusqlite::types::Value>, column: usize| match row.get(column) {
        Some(rusqlite::types::Value::Integer(value)) => *value,
        _ => -1,
    };
    assert!(
        program[..upper]
            .iter()
            .any(|row| opcode(row) == "Once" && integer(row, 2) > integer(&program[upper], 0)),
        "{program:?}"
    );
}

/// Like SQLite, a query with exactly one min() or max() aggregate reads its
/// bare columns from a row that holds the aggregate's value: the first row
/// until a value is held, then each row with a new value. A tie, or a value
/// equal under the collation, keeps the earlier row. Legacy reads them from
/// the first row.
#[test]
fn hir_bare_columns_come_from_the_min_or_max_row() {
    const SCHEMA: &str = "
        CREATE TABLE t(g INT, a INT, b TEXT);
        INSERT INTO t VALUES (1, 5, 'x'), (1, 9, 'y'), (1, 2, 'z'), (2, 1, 'p'), (2, 7, 'q'),
            (2, 7, 'r');
        CREATE TABLE n(a, b);
        INSERT INTO n VALUES (NULL, 'r1'), (NULL, 'r2'), (3, 'r3'), (NULL, 'r4'), (3, 'r5');
        CREATE TABLE c(a TEXT, b INT);
        INSERT INTO c VALUES ('A', 1), ('a', 2), ('B', 3), ('b', 4);
    ";
    let db = TempDatabase::new_empty();
    let hir = db.connect_limbo();
    for statement in SCHEMA.split(';').filter(|sql| !sql.trim().is_empty()) {
        hir.execute(statement).unwrap();
    }
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch(SCHEMA).unwrap();
    for sql in [
        "SELECT max(a), b FROM t",
        "SELECT min(a), b, rowid FROM t",
        "SELECT g, max(a), b FROM t GROUP BY g ORDER BY g",
        "SELECT g, min(a), b FROM t GROUP BY g ORDER BY g",
        "SELECT g, b FROM t GROUP BY g HAVING max(a) > 0 ORDER BY g",
        "SELECT max(a), count(*), b FROM t",
        "SELECT count(*), b FROM t",
        "SELECT max(a), b FROM n",
        "SELECT min(a), b FROM n WHERE a IS NULL",
        "SELECT max(a) + 1, b || '!' FROM t",
        "SELECT max(a COLLATE NOCASE), b FROM c",
        "SELECT max(a) FILTER (WHERE g = 2), b FROM t",
        "SELECT max(DISTINCT a), b FROM t",
        "SELECT max(a), (SELECT b) FROM t",
        "SELECT max(t.a), n.b FROM t JOIN n ON n.a = 3",
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
        assert_eq!(limbo_exec_rows(&hir, sql), expected, "{sql}");
    }
}

/// Like SQLite, an aggregate's arguments may be ordered: `group_concat(a
/// ORDER BY b)` keeps each row's ORDER BY values and arguments in an
/// ephemeral index and steps through it in order when it is finalized, per
/// group in a grouped query. Tied rows keep their order, and DISTINCT keeps
/// the first of equal arguments in that order. Legacy rejects it.
#[test]
fn hir_aggregate_arguments_follow_their_order_by() {
    const SCHEMA: &str = "
        CREATE TABLE t(g, a, b, c TEXT);
        INSERT INTO t VALUES (1, 'x', 3, 'B'), (1, 'y', 1, 'a'), (2, 'z', 2, 'A'),
            (1, 'w', 2, 'b'), (2, 'v', NULL, 'c'), (1, 'x', 1, 'C');
    ";
    let db = TempDatabase::new_empty();
    let hir = db.connect_limbo();
    for statement in SCHEMA.split(';').filter(|sql| !sql.trim().is_empty()) {
        hir.execute(statement).unwrap();
    }
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch(SCHEMA).unwrap();
    for sql in [
        "SELECT group_concat(a ORDER BY b) FROM t",
        "SELECT g, group_concat(a, '-' ORDER BY b DESC) FROM t GROUP BY g ORDER BY g",
        "SELECT json_group_array(a ORDER BY b, a DESC) FROM t",
        "SELECT group_concat(a ORDER BY b NULLS LAST) FROM t",
        "SELECT group_concat(c ORDER BY c COLLATE NOCASE) FROM t",
        "SELECT group_concat(DISTINCT a ORDER BY a DESC) FROM t",
        "SELECT group_concat(a ORDER BY b) FILTER (WHERE g = 1) FROM t",
        "SELECT group_concat(a ORDER BY g) FROM t",
        "SELECT count(*), group_concat(a ORDER BY b), sum(b) FROM t",
        "SELECT group_concat(a ORDER BY b) FROM t WHERE 0",
        "SELECT g FROM t GROUP BY g HAVING group_concat(a ORDER BY b) LIKE 'y%' ORDER BY g",
        "SELECT (SELECT group_concat(a ORDER BY b) FROM t t2 WHERE t2.g = t.g) FROM t \
         GROUP BY g ORDER BY g",
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
        assert_eq!(limbo_exec_rows(&hir, sql), expected, "{sql}");
    }
}

/// Like legacy, the join search starts from the FROM-order plan and only a
/// strictly cheaper order replaces it. Equally cheap orders resolve the same
/// way as legacy, so both pick the same loops for these joins.
#[test]
fn hir_equally_cheap_join_orders_keep_the_from_order() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE customers(customer_id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)",
        "CREATE TABLE products(product_id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)",
        "CREATE TABLE orders(order_id INTEGER PRIMARY KEY NOT NULL, \
         customer_id INTEGER NOT NULL, order_date DATE NOT NULL)",
        "CREATE TABLE order_items(item_id INTEGER PRIMARY KEY NOT NULL, \
         order_id INTEGER NOT NULL, product_id INTEGER NOT NULL, quantity INTEGER NOT NULL)",
        "CREATE INDEX idx_orders_customer_id ON orders(customer_id)",
        "CREATE INDEX idx_order_items_order_id ON order_items(order_id)",
        "CREATE INDEX idx_order_items_product_id ON order_items(product_id)",
        "INSERT INTO customers VALUES (1,'ann'),(2,'bob')",
        "INSERT INTO products VALUES (1,'pen'),(2,'cup')",
        "INSERT INTO orders VALUES (1,1,'d1'),(2,2,'d2'),(3,1,'d3')",
        "INSERT INTO order_items VALUES (1,1,1,3),(2,1,2,1),(3,2,2,5),(4,3,1,2)",
    ] {
        hir.execute(statement).unwrap();
    }
    for sql in [
        "SELECT c.name, o.order_id, p.name, oi.quantity \
         FROM customers c, orders o, order_items oi, products p \
         WHERE c.customer_id = o.customer_id AND o.order_id = oi.order_id \
         AND oi.product_id = p.product_id",
        "SELECT c.name, o.order_id, p.name, oi.quantity FROM customers c \
         JOIN orders o ON c.customer_id = o.customer_id \
         JOIN order_items oi ON o.order_id = oi.order_id \
         JOIN products p ON oi.product_id = p.product_id",
    ] {
        let plan = query_plan(&hir, sql);
        assert_eq!(plan, expected_plan(sql), "{sql}");
        assert_eq!(
            plan,
            [
                "SCAN order_items AS oi",
                "SEARCH p USING INTEGER PRIMARY KEY (rowid=?)",
                "SEARCH o USING INTEGER PRIMARY KEY (rowid=?)",
                "SEARCH c USING INTEGER PRIMARY KEY (rowid=?)",
            ],
            "{sql}"
        );
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// Like legacy's `optimizer/unnest.rs`, a correlated EXISTS or IN becomes a
/// semi-join and a correlated NOT EXISTS an anti-join, when that costs no
/// more than running the subquery for every outer row. An anti-join whose
/// inner table has no index on the link column hashes the outer table and
/// scans the inner one, marking the outer rows that match; the others come
/// out after the scan. A semi-join seeks a temporary index instead, since
/// it stops at the first match.
#[test]
fn hir_correlated_exists_and_in_become_semi_and_anti_joins() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE o(id INTEGER PRIMARY KEY, k1 INT, k2 INT, body TEXT)",
        "CREATE TABLE i(k1 INT, v INT)",
        "CREATE TABLE j(k2 INT)",
        "CREATE TABLE p(id INTEGER PRIMARY KEY, k INT)",
        "CREATE INDEX p_k ON p(k)",
        "INSERT INTO o VALUES (1,1,1,'apple'),(2,1,2,'pear'),(3,2,1,'apple pie'),\
         (4,NULL,1,'fig'),(5,3,NULL,'apple')",
        "INSERT INTO i VALUES (1,10),(1,11),(3,NULL),(NULL,5)",
        "INSERT INTO j VALUES (1),(5),(NULL)",
        "INSERT INTO p VALUES (1,1),(2,3)",
    ] {
        hir.execute(statement).unwrap();
    }
    // The plans name automatic indexes after their table's position in the
    // statement, which HIR and legacy count differently.
    let plan = |rows: Vec<String>| {
        rows.into_iter()
            .map(|line| {
                let mut words = line.split(' ').map(str::to_string).collect::<Vec<_>>();
                for word in &mut words {
                    if word.starts_with("ephemeral_") {
                        let end = word.rfind('_').unwrap();
                        word.truncate(end);
                    }
                }
                words.join(" ")
            })
            .collect::<Vec<_>>()
    };
    for (sql, expected) in [
        (
            "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1) \
             AND EXISTS (SELECT 1 FROM j WHERE j.k2 = o.k2)",
            [
                "SCAN o",
                "SEARCH i USING COVERING INDEX ephemeral_i (k1=?)",
                "SEARCH j USING COVERING INDEX ephemeral_j (k2=?)",
            ]
            .as_slice(),
        ),
        (
            "SELECT o.id FROM o WHERE o.body LIKE 'apple%' \
             AND EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1)",
            &["SCAN o", "SEARCH i USING COVERING INDEX ephemeral_i (k1=?)"],
        ),
        (
            "SELECT o.id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1)",
            &["HASH JOIN i", "SCAN o"],
        ),
        // The second anti-join cannot hash: its build table would be the
        // first anti-join's inner table.
        (
            "SELECT o.id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1) \
             AND NOT EXISTS (SELECT 1 FROM j WHERE j.k2 = o.k2)",
            &[
                "HASH JOIN i",
                "SEARCH j USING COVERING INDEX ephemeral_j (k2=?)",
                "SCAN o",
            ],
        ),
        (
            "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1) \
             AND NOT EXISTS (SELECT 1 FROM j WHERE j.k2 = o.k2)",
            &[
                "SCAN o",
                "SEARCH i USING COVERING INDEX ephemeral_i (k1=?)",
                "SEARCH j USING COVERING INDEX ephemeral_j (k2=?)",
            ],
        ),
        (
            "SELECT o.id FROM o WHERE o.k2 IN (SELECT j.k2 FROM j WHERE j.k2 = o.k1)",
            &["SCAN o", "SEARCH j USING COVERING INDEX ephemeral_j (k2=?)"],
        ),
        (
            "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM p WHERE p.id = o.k1)",
            &["SCAN o", "SEARCH p USING INTEGER PRIMARY KEY (rowid=?)"],
        ),
        (
            "SELECT o.id, count(*) FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1) \
             GROUP BY o.id",
            &["SCAN o", "SEARCH i USING COVERING INDEX ephemeral_i (k1=?)"],
        ),
        // A link that is not `=` also moves into the join, where it drives
        // a range seek.
        (
            "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k1 < o.k1)",
            &["SCAN o", "SEARCH i USING COVERING INDEX ephemeral_i (k1<?)"],
        ),
    ] {
        assert_eq!(plan(query_plan(&hir, sql)), expected, "{sql}");
        assert_same_rows(&hir, &sqlite, sql);
    }
    // These stay subqueries, as in legacy: a link to the NULL row of a LEFT
    // JOIN, NOT IN, an OR, and a NOT EXISTS term that does not read the
    // inner table.
    for sql in [
        "SELECT o.id, p.id FROM o LEFT JOIN p ON p.k = o.k1 \
         WHERE EXISTS (SELECT 1 FROM i WHERE i.k1 = p.id)",
        "SELECT o.id FROM o WHERE o.k2 NOT IN (SELECT j.k2 FROM j WHERE j.k2 = o.k1)",
        "SELECT o.id FROM o WHERE o.k1 = 2 OR EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1)",
        "SELECT o.id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1 AND o.k2 = 1)",
    ] {
        let hir_plan = plan(query_plan(&hir, sql));
        assert_eq!(hir_plan, plan(expected_plan(sql)), "{sql}");
        // A semi-join or anti-join would seek i or j through an automatic
        // index; the subquery scans it.
        assert!(
            !hir_plan.iter().any(|line| line.contains("ephemeral")),
            "{sql}: {hir_plan:?}"
        );
        assert_same_rows(&hir, &sqlite, sql);
    }
    // Rows still match legacy when the moved source is a FROM subquery or
    // CTE, when the subquery holds an uncorrelated subquery, when the
    // outer block groups or reads a FROM subquery, and when the inner
    // table is empty.
    hir.execute("CREATE TABLE e(k INT)").unwrap();
    for sql in [
        "SELECT o.id FROM o WHERE EXISTS \
         (SELECT 1 FROM (SELECT k1 FROM i WHERE v > 5) d WHERE d.k1 = o.k1)",
        "WITH c AS (SELECT k1 FROM i WHERE v IS NOT NULL) \
         SELECT o.id FROM o WHERE NOT EXISTS (SELECT 1 FROM c WHERE c.k1 = o.k1)",
        "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1 \
         AND i.v IN (SELECT v FROM i WHERE v > 10))",
        "SELECT o.body, count(*) FROM o WHERE o.k2 IN (SELECT j.k2 FROM j WHERE j.k2 = o.k2) \
         GROUP BY o.body ORDER BY 1",
        "SELECT x.id FROM (SELECT id, k1 FROM o WHERE id > 1) x \
         WHERE EXISTS (SELECT 1 FROM i WHERE i.k1 = x.k1) ORDER BY 1",
        "SELECT o.id FROM o WHERE NOT EXISTS (SELECT 1 FROM e WHERE e.k = o.k1) ORDER BY 1",
        "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM e WHERE e.k = o.k1)",
        "SELECT o.id, p.id FROM o JOIN p ON p.k = o.k1 \
         WHERE NOT EXISTS (SELECT 1 FROM j WHERE j.k2 = o.k2) ORDER BY 1",
        "SELECT (SELECT count(*) FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1)), \
         (SELECT group_concat(id) FROM o WHERE NOT EXISTS (SELECT 1 FROM j WHERE j.k2 = o.k2))",
    ] {
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// Like legacy's group-first rewrite, a correlated one-value aggregate
/// becomes a FROM subquery grouped by its link columns and LEFT JOINed on
/// them. Like legacy, a LEFT JOIN after an ordered scan keeps the scan's
/// order for ORDER BY terms that do not read its columns, and a LEFT JOIN
/// the WHERE clause turns into an inner join is reported as one.
#[test]
fn hir_correlated_aggregates_group_first() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE o(id INTEGER PRIMARY KEY, k1 INT, k2 INT, amount INT, t TEXT)",
        "CREATE TABLE i(k1 INT, k2 INT, amount INT, t TEXT COLLATE NOCASE)",
        "INSERT INTO o VALUES (1,1,10,5,'A'),(2,1,10,15,'a'),(3,2,20,7,'B'),(4,3,30,9,'C'),\
         (5,NULL,40,4,NULL),(6,2,NULL,NULL,'b')",
        "INSERT INTO i VALUES (1,10,10,'a'),(1,10,20,'A'),(1,99,100,'x'),(2,20,6,'b'),\
         (2,20,NULL,'B'),(2,NULL,7,NULL),(NULL,40,99,NULL)",
    ] {
        hir.execute(statement).unwrap();
    }
    let plan = |conn, sql| {
        query_plan(conn, sql)
            .into_iter()
            .map(|line| {
                // Legacy and HIR number subqueries and automatic indexes
                // differently.
                line.split(' ')
                    .map(|word| match word.rfind('_') {
                        Some(end)
                            if word.starts_with("ephemeral_")
                                || word.starts_with("scalar_subquery_") =>
                        {
                            &word[..end]
                        }
                        _ => word,
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>()
    };
    for (sql, expected) in [
        (
            "SELECT id, (SELECT avg(i.amount) FROM i WHERE i.k1 = o.k1) FROM o ORDER BY id",
            [
                "SCAN o",
                "SEARCH scalar_subquery USING INDEX ephemeral_subquery (correlation_key_0=?) \
                 LEFT-JOIN",
                "SCAN i",
                "USE SORTER FOR GROUP BY",
            ]
            .as_slice(),
        ),
        (
            "SELECT o.id FROM o WHERE o.amount < (SELECT avg(i.amount) FROM i WHERE i.k1 = o.k1)",
            &[
                "SCAN o",
                "SEARCH scalar_subquery USING INDEX ephemeral_subquery (correlation_key_0=?)",
                "SCAN i",
                "USE SORTER FOR GROUP BY",
            ],
        ),
    ] {
        assert_eq!(plan(&hir, sql), expected, "{sql}");
    }
    for sql in [
        "SELECT id, (SELECT avg(i.amount) FROM i WHERE i.k1 = o.k1) FROM o ORDER BY id",
        "SELECT id, (SELECT count(*) FROM i WHERE i.k1 = o.k1) FROM o ORDER BY id",
        "SELECT id, (SELECT total(i.amount) FROM i WHERE i.k1 = o.k1) FROM o ORDER BY id",
        "SELECT id, (SELECT min(i.amount) FROM i WHERE i.k1 = o.k1 AND i.k2 = o.k2) \
         FROM o ORDER BY id",
        "SELECT id, (SELECT max(i.amount) + 1 FROM i WHERE o.k1 = i.k1 AND i.amount > 6) \
         FROM o ORDER BY id",
        "SELECT o.id FROM o WHERE o.amount < (SELECT avg(i.amount) FROM i WHERE i.k1 = o.k1) \
         ORDER BY 1",
        "SELECT o.k1, count(*), (SELECT max(i.amount) FROM i WHERE i.k1 = o.k1) FROM o \
         GROUP BY o.k1 ORDER BY 1",
        "SELECT o.k1 FROM o GROUP BY o.k1 \
         HAVING count(*) < (SELECT count(*) FROM i WHERE i.k1 = o.k1) ORDER BY 1",
        "SELECT o.id FROM o ORDER BY (SELECT count(*) FROM i WHERE i.k1 = o.k1), o.id",
        // An aggregate subquery read by IN or EXISTS stays a subquery.
        "SELECT o.id FROM o WHERE o.amount IN (SELECT max(i.amount) FROM i WHERE i.k1 = o.k1) \
         OR o.id = 1 ORDER BY 1",
        "SELECT o.id FROM o WHERE NOT EXISTS (SELECT count(*) FROM i WHERE i.k1 = o.k1) \
         OR o.id = 2 ORDER BY 1",
        // Different text orders keep the subquery correlated.
        "SELECT o.id, (SELECT count(*) FROM i WHERE i.t = o.t) FROM o ORDER BY 1",
        "SELECT o.id, (SELECT sum(i.amount) FROM i WHERE i.k1 = o.k1) FROM o ORDER BY 1",
    ] {
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// Like legacy: when computing an aggregate for keys no outer row asks for
/// could fail, as `sum` can overflow, a direct WHERE comparison joins the
/// inner table first and groups by the outer rowid. A second copy of a
/// rewritten subquery in the same clause reads the same grouped table.
#[test]
fn hir_correlated_aggregates_join_first_and_share_grouped_tables() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE o(id INTEGER PRIMARY KEY, k INT, max_sum INT, x REAL)",
        "CREATE TABLE i(k INT, x INT, q REAL)",
        "INSERT INTO o VALUES (1,1,11,0.5),(2,1,9,1.5),(3,2,1,7.0),(4,NULL,1,0.1),(5,3,1,2.0)",
        "INSERT INTO i VALUES (1,5,1.0),(1,5,2.0),(2,NULL,8.0),\
         (99,9223372036854775807,1.0),(99,9223372036854775807,1.0)",
    ] {
        hir.execute(statement).unwrap();
    }
    let sum_sql = "SELECT o.id FROM o WHERE o.max_sum > (SELECT sum(i.x) FROM i WHERE i.k = o.k)";
    let plan = query_plan(&hir, sum_sql);
    assert_eq!(plan, expected_plan(sum_sql));
    assert_eq!(plan, ["HASH JOIN i", "SCAN o", "USE SORTER FOR GROUP BY"]);
    let shared_sql = "SELECT count(*) FROM o WHERE o.x < (SELECT avg(i.q) FROM i WHERE i.k = o.k) \
                      AND o.x > (SELECT avg(i.q) FROM i WHERE i.k = o.k) - 100";
    let plan = query_plan(&hir, shared_sql);
    assert_eq!(
        plan.iter()
            .filter(|line| line.starts_with("SEARCH scalar_subquery_"))
            .count(),
        1,
        "{plan:?}"
    );
    for sql in [
        sum_sql,
        "SELECT o.id FROM o WHERE (SELECT sum(i.x) FROM i WHERE i.k = o.k) < o.max_sum ORDER BY 1",
        "SELECT o.id, o.max_sum FROM o WHERE o.max_sum >= \
         (SELECT count(i.x) + sum(i.x) FROM i WHERE i.k = o.k AND i.x > 1) AND o.id > 0",
        "SELECT o.id FROM o WHERE o.max_sum > (SELECT group_concat(i.x) FROM i WHERE i.k = o.k)",
        shared_sql,
        "SELECT o.id, (SELECT avg(i.q) FROM i WHERE i.k = o.k), \
         (SELECT avg(i.q) FROM i WHERE i.k = o.k) FROM o \
         WHERE o.x < (SELECT avg(i.q) FROM i WHERE i.k = o.k) ORDER BY 1",
    ] {
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// Like legacy's hash join, HIR builds the hash table from the table joined
/// before and scans the next one, probing it, so both plans hash and scan
/// the same tables. A LEFT join builds from its left table and then emits
/// the left rows nothing matched.
#[test]
fn hir_hash_joins_build_and_probe() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE h_orders(order_id INTEGER PRIMARY KEY, customer_name TEXT, amount INT)",
        "CREATE TABLE h_items(item_id INTEGER PRIMARY KEY, order_name TEXT, product TEXT, \
         quantity INT)",
        "INSERT INTO h_orders VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,NULL,5)",
        "INSERT INTO h_items VALUES (1,'a','pen',1),(2,'a','cup',2),(3,'b','ink',3),\
         (4,'z','box',4),(5,NULL,'nil',5)",
    ] {
        hir.execute(statement).unwrap();
    }
    for (sql, expected) in [
        (
            "SELECT h.order_id, i.product FROM h_orders h \
             JOIN h_items i ON h.customer_name = i.order_name ORDER BY 1, 2",
            [
                "HASH JOIN h_items AS i",
                "SCAN h_orders AS h",
                "USE SORTER FOR ORDER BY",
            ]
            .as_slice(),
        ),
        (
            "SELECT h.order_id, i.product FROM h_orders h \
             LEFT JOIN h_items i ON h.customer_name = i.order_name ORDER BY 1, 2",
            &[
                "HASH JOIN h_items AS i",
                "SCAN h_orders AS h",
                "USE SORTER FOR ORDER BY",
            ],
        ),
        // A FULL JOIN never hashes: its right rows nothing matched come
        // from a pass over the right table after the loops.
        (
            "SELECT h.order_id, i.item_id FROM h_orders h \
             FULL JOIN h_items i ON h.customer_name = i.order_name ORDER BY 1, 2",
            &[
                "SCAN h_orders AS h",
                "SCAN h_items AS i LEFT-JOIN",
                "RIGHT-JOIN i",
                "SCAN h_items AS i",
                "USE SORTER FOR ORDER BY",
            ],
        ),
        (
            "SELECT h.customer_name, count(i.item_id) FROM h_orders h \
             LEFT JOIN h_items i ON h.customer_name = i.order_name \
             GROUP BY h.customer_name ORDER BY 1",
            &[
                "HASH JOIN h_items AS i",
                "SCAN h_orders AS h",
                "USE SORTER FOR GROUP BY",
            ],
        ),
        (
            // The key may be an expression of the build table.
            "SELECT h.order_id, i.item_id FROM h_orders h \
             JOIN h_items i ON i.quantity = h.amount / 10 ORDER BY 1, 2",
            &[
                "HASH JOIN h_items AS i",
                "SCAN h_orders AS h",
                "USE SORTER FOR ORDER BY",
            ],
        ),
    ] {
        let plan = query_plan(&hir, sql);
        assert_eq!(plan, expected_plan(sql), "{sql}");
        assert_eq!(plan, expected, "{sql}");
    }
    for sql in [
        "SELECT h.order_id, i.product FROM h_orders h \
         LEFT JOIN h_items i ON h.customer_name = i.order_name AND i.quantity > 1 ORDER BY 1, 2",
        "SELECT h.order_id FROM h_orders h \
         LEFT JOIN h_items i ON h.customer_name = i.order_name WHERE i.item_id IS NULL ORDER BY 1",
        "SELECT h.order_id, i.item_id FROM h_orders h \
         FULL JOIN h_items i ON h.customer_name = i.order_name \
         WHERE h.amount > 5 OR i.quantity > 3 ORDER BY 1, 2",
        "SELECT h.order_id, i.item_id FROM h_orders h \
         FULL JOIN h_items i ON h.customer_name = i.order_name \
         WHERE h.order_id IS NULL OR i.item_id IS NULL ORDER BY 1, 2",
        "SELECT count(*), count(h.order_id), count(i.item_id) FROM h_orders h \
         FULL JOIN h_items i ON h.customer_name = i.order_name",
    ] {
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// A correlated EXISTS, NOT EXISTS or IN also becomes a semi-join or
/// anti-join in a block with a RIGHT or FULL join, where the rest of the
/// block runs again for the unmatched rows; in an EXISTS whose SELECT list
/// holds a parameter or an uncorrelated subquery, which are dropped with
/// that list while the parameter still counts; and when the subquery reads
/// a table-valued function, whose arguments move with it.
#[test]
fn hir_semi_and_anti_joins_cover_outer_joins_parameters_and_table_functions() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE o(id INTEGER PRIMARY KEY, k INT, v INT)",
        "CREATE TABLE p(id INTEGER PRIMARY KEY, k INT)",
        "CREATE TABLE i(k INT, x INT)",
        "CREATE TABLE d(x INT)",
        "INSERT INTO o VALUES (1,1,10),(2,2,20),(3,3,30),(4,NULL,40)",
        "INSERT INTO p VALUES (1,1),(2,5),(3,3)",
        "INSERT INTO i VALUES (1,100),(3,300),(5,500),(NULL,0)",
        "INSERT INTO d VALUES (7),(9)",
        // Large enough tables that each join costs less than its subquery.
        "ANALYZE",
        "DELETE FROM sqlite_stat1",
        "INSERT INTO sqlite_stat1(tbl, idx, stat) VALUES ('o', NULL, '1000'), \
         ('p', NULL, '1000'), ('i', NULL, '100000'), ('d', NULL, '10')",
        "ANALYZE sqlite_schema",
    ] {
        hir.execute(statement).unwrap();
    }
    // The loop of a semi-join or anti-join marks where it stops.
    let semi_join = |sql: &str, table: &str| {
        let program = limbo_exec_rows(&hir, &format!("EXPLAIN {sql}"));
        assert!(
            program.iter().any(|row| matches!(
                row.last(),
                Some(rusqlite::types::Value::Text(comment))
                    if comment.contains("semi-join") || comment.contains("anti-join")
            )),
            "{sql}: {program:?}"
        );
        let plan = query_plan(&hir, sql);
        assert!(
            plan.iter().any(|line| line.contains(table)),
            "{sql}: {plan:?}"
        );
    };
    for (sql, table) in [
        (
            "SELECT o.id, p.id FROM o FULL JOIN p ON p.k = o.k \
             WHERE EXISTS (SELECT 1 FROM i WHERE i.k = o.k) ORDER BY 1, 2",
            "i",
        ),
        (
            "SELECT o.id, p.id FROM o FULL JOIN p ON p.k = o.k \
             WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.k = p.k) ORDER BY 1, 2",
            "i",
        ),
        (
            "SELECT o.id, p.id FROM o RIGHT JOIN p ON p.k = o.k \
             WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.k = p.k) ORDER BY 1, 2",
            "i",
        ),
        (
            "SELECT o.id, p.id FROM o RIGHT JOIN p ON p.k = o.k \
             WHERE p.k IN (SELECT i.k FROM i WHERE i.k = p.id) ORDER BY 1, 2",
            "i",
        ),
        (
            "SELECT o.id FROM o WHERE EXISTS \
             (SELECT (SELECT max(x) FROM d) FROM i WHERE i.k = o.k) ORDER BY 1",
            "i",
        ),
        (
            "SELECT o.id FROM o WHERE EXISTS \
             (SELECT 1 FROM generate_series(1, o.k) g WHERE g.value = 2) ORDER BY 1",
            "generate_series AS g",
        ),
        (
            "SELECT o.id FROM o WHERE o.k IS NOT NULL AND NOT EXISTS \
             (SELECT 1 FROM generate_series(o.k, 2) g WHERE g.value = o.k) ORDER BY 1",
            "generate_series AS g",
        ),
    ] {
        semi_join(sql, table);
        if sql.contains("generate_series") {
            // SQLite as built here has no generate_series.
            let rows = limbo_exec_rows(&hir, sql);
            let expected: &[i64] = if sql.contains("NOT EXISTS") {
                &[3]
            } else {
                &[2, 3]
            };
            assert_eq!(
                rows,
                expected
                    .iter()
                    .map(|id| vec![rusqlite::types::Value::Integer(*id)])
                    .collect::<Vec<_>>(),
                "{sql}"
            );
        } else {
            assert_same_rows(&hir, &sqlite, sql);
        }
    }

    // The semi-join's loop runs inside the FULL JOIN's loops, and again in
    // the pass over the right rows nothing matched. Temporary index names
    // end in different ids.
    let sql = "SELECT o.id, p.id FROM o FULL JOIN p ON p.k = o.k \
               WHERE EXISTS (SELECT 1 FROM i WHERE i.k = o.k) ORDER BY 1, 2";
    let plan = plan_without_ids(query_plan(&hir, sql));
    assert_eq!(plan, plan_without_ids(expected_plan(sql)), "{sql}");
    assert_eq!(
        plan,
        [
            "SCAN o",
            "SCAN p LEFT-JOIN",
            "SEARCH i USING COVERING INDEX ephemeral_i (k=?)",
            "RIGHT-JOIN p",
            "SCAN p",
            "USE SORTER FOR ORDER BY",
        ],
        "{sql}"
    );

    // Like legacy, a link to the side a RIGHT JOIN fills with NULL keeps the
    // subquery.
    let sql = "SELECT o.id, p.id FROM o RIGHT JOIN p ON p.k = o.k \
               WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.k = o.k) ORDER BY 1, 2";
    let program = limbo_exec_rows(&hir, &format!("EXPLAIN {sql}"));
    assert!(
        !program.iter().any(|row| matches!(
            row.last(),
            Some(rusqlite::types::Value::Text(comment)) if comment.contains("anti-join")
        )),
        "{sql}: {program:?}"
    );
    assert_same_rows(&hir, &sqlite, sql);

    // The parameter of the dropped SELECT list still counts and binds.
    let sql = "SELECT o.id FROM o WHERE EXISTS (SELECT ?1 FROM i WHERE i.k = o.k) \
               AND o.v > ?2 ORDER BY 1";
    semi_join(sql, "i");
    let run = |conn: &std::sync::Arc<turso_core::Connection>| {
        let mut stmt = conn.prepare(sql).unwrap();
        assert_eq!(stmt.parameters_count(), 2);
        stmt.bind_at(1.try_into().unwrap(), turso_core::Value::from_i64(5))
            .unwrap();
        stmt.bind_at(2.try_into().unwrap(), turso_core::Value::from_i64(15))
            .unwrap();
        let mut rows = Vec::new();
        stmt.run_with_row_callback(|row| {
            rows.push(row.get::<i64>(0).unwrap());
            Ok(())
        })
        .unwrap();
        rows
    };
    assert_eq!(run(&hir), vec![3]);
}

/// An outer hash join large enough for its hash table to move rows to disk
/// records thousands of matched build rows. Each goes into a temporary rowid
/// table as a one-column record, since an empty record with a small rowid
/// makes a cell smaller than any B-tree page can hold.
#[test]
fn hir_outer_hash_joins_record_many_matched_rows() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE b(id INTEGER PRIMARY KEY, k INT, pad TEXT)",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, k INT, pad TEXT)",
        "INSERT INTO b SELECT value, CASE WHEN value % 13 THEN value % 500 END, \
         printf('%050d', value) FROM generate_series(1, 3000)",
        "INSERT INTO c SELECT value, CASE WHEN value % 13 THEN value % 700 END, \
         printf('%050d', value) FROM generate_series(1, 3000)",
    ] {
        hir.execute(statement).unwrap();
    }
    for (sql, plan, expected) in [
        (
            "SELECT count(*), sum(b.id), sum(c.id) FROM b LEFT JOIN c ON b.k = c.k",
            ["HASH JOIN c", "SCAN b"].as_slice(),
            (11496, 17079801, 16257256),
        ),
        // A FULL JOIN never hashes.
        (
            "SELECT count(*), sum(b.id), sum(c.id) FROM b FULL JOIN c ON b.k = c.k",
            &["SCAN b", "SCAN c LEFT-JOIN", "RIGHT-JOIN c", "SCAN c"],
            (12465, 17079801, 17820541),
        ),
    ] {
        assert_eq!(query_plan(&hir, sql), plan, "{sql}");
        let rows = limbo_exec_rows(&hir, sql);
        assert_eq!(rows, sqlite.rows(sql), "{sql}");
        assert_eq!(
            rows,
            [[
                rusqlite::types::Value::Integer(expected.0),
                rusqlite::types::Value::Integer(expected.1),
                rusqlite::types::Value::Integer(expected.2),
            ]],
            "{sql}"
        );
    }
}

/// Like legacy, a hash join may scan its probe table under earlier loops:
/// the hash table is built once, and each pass of the probe loop joins its
/// own rows, including the rows that waited for a part of the table on
/// disk. A LEFT hash join starts each pass with no build row matched and
/// ends it with the build rows nothing matched, next to the earlier loops'
/// rows. A FULL join never hashes. The statistics count one row in `a`, so
/// the probe scan under its loop is priced as running once and the hash
/// join wins; the loop really runs three times.
#[test]
fn hir_hash_joins_run_under_earlier_loops() {
    const SCHEMA: &str = "
        CREATE TABLE a(id INTEGER PRIMARY KEY, x INT);
        CREATE TABLE b(id INTEGER PRIMARY KEY, k INT, pad TEXT);
        CREATE TABLE c(id INTEGER PRIMARY KEY, k INT, pad TEXT);
        CREATE TABLE d(id INTEGER PRIMARY KEY, k INT);
        INSERT INTO a VALUES (1, 1), (2, 2), (3, 3);
        WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 3000)
            INSERT INTO b SELECT i, CASE WHEN i % 13 THEN i % 500 END, printf('%050d', i) FROM s;
        WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 3000)
            INSERT INTO c SELECT i, CASE WHEN i % 13 THEN i % 700 END, printf('%050d', i) FROM s;
        WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 2000)
            INSERT INTO d SELECT i, i % 300 FROM s;
    ";
    let db = TempDatabase::new_empty();
    let (_, hir) = connections(&db);
    for statement in SCHEMA.split(';').filter(|sql| !sql.trim().is_empty()) {
        hir.execute(statement).unwrap();
    }
    for statement in [
        "ANALYZE",
        "DELETE FROM sqlite_stat1",
        "INSERT INTO sqlite_stat1(tbl, idx, stat) VALUES ('a', NULL, '1'), \
         ('b', NULL, '3000'), ('c', NULL, '3000'), ('d', NULL, '2000')",
        "ANALYZE sqlite_schema",
    ] {
        hir.execute(statement).unwrap();
    }
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch(SCHEMA).unwrap();
    for (sql, plan) in [
        (
            "SELECT count(*), sum(b.id), sum(c.id) FROM a, b JOIN c ON b.k = c.k",
            Some(["SCAN a", "HASH JOIN c", "SCAN b"].as_slice()),
        ),
        (
            "SELECT count(*), sum(a.id), sum(b.id), sum(c.id) FROM a, b LEFT JOIN c ON b.k = c.k",
            Some(&["SCAN a", "HASH JOIN c", "SCAN b"]),
        ),
        (
            "SELECT count(*), sum(a.id), sum(b.id), sum(c.id) FROM a, b \
             LEFT JOIN c ON b.k = c.k AND c.id % 3 = a.id - 1 WHERE c.id IS NULL OR a.id = 2",
            Some(&["SCAN a", "HASH JOIN c", "SCAN b"]),
        ),
        (
            "SELECT count(*), sum(a.id), sum(b.id), sum(c.id), sum(d.id) FROM a \
             LEFT JOIN d ON d.id = a.x * 1000, b LEFT JOIN c ON b.k = c.k",
            Some(&[
                "SCAN a",
                "SEARCH d USING INTEGER PRIMARY KEY (rowid=?) LEFT-JOIN",
                "HASH JOIN c",
                "SCAN b",
            ]),
        ),
        (
            "SELECT a.id, (SELECT count(*) FROM b LEFT JOIN c ON b.k = c.k \
             AND c.id % 3 = a.id - 1) FROM a ORDER BY a.id",
            None,
        ),
        (
            "SELECT count(*), sum(a.id), sum(b.id), sum(c.id) FROM a, b FULL JOIN c ON b.k = c.k",
            None,
        ),
    ] {
        let hir_plan = query_plan(&hir, sql);
        match plan {
            Some(plan) => {
                assert_eq!(hir_plan, plan, "{sql}");
                assert_eq!(hir_plan, expected_plan(sql), "{sql}");
            }
            None => assert_eq!(
                hir_plan.iter().any(|line| line.starts_with("HASH JOIN")),
                !sql.contains("FULL"),
                "{sql}: {hir_plan:?}"
            ),
        }
        let mut stmt = sqlite.prepare(sql).unwrap();
        let columns = stmt.column_count();
        let expected: Vec<Vec<rusqlite::types::Value>> = stmt
            .query_map([], |row| {
                (0..columns).map(|column| row.get(column)).collect()
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(limbo_exec_rows(&hir, sql), expected, "{sql}");
    }
}

/// Like legacy's materialized hash build input: when the build table was
/// the probe of an earlier hash join, or has a term with an earlier table
/// that the hash keys do not cover, the loops before the probe run once
/// into a temporary table that the hash table is built from, and each hash
/// row carries the columns of every table those loops read. The block's
/// loops are then the probe scan and the lookup alone, and the query reads
/// those tables from the hash row: in its outputs, aggregates, GROUP BY and
/// WHERE terms, and in a later loop's seek.
#[test]
fn hir_hash_joins_build_from_the_loops_before_the_probe() {
    const SCHEMA: &str = "
        CREATE TABLE b(id INTEGER PRIMARY KEY, k INT, pad TEXT);
        CREATE TABLE c(id INTEGER PRIMARY KEY, k INT, pad TEXT);
        CREATE TABLE d(id INTEGER PRIMARY KEY, k INT);
        CREATE TABLE e(id INTEGER PRIMARY KEY, v INT);
        WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 3000)
            INSERT INTO b SELECT i, CASE WHEN i % 13 THEN i % 500 END, printf('%050d', i) FROM s;
        WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 3000)
            INSERT INTO c SELECT i, CASE WHEN i % 13 THEN i % 700 END, printf('%050d', i) FROM s;
        WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 2000)
            INSERT INTO d SELECT i, i % 300 FROM s;
        WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 3000)
            INSERT INTO e SELECT i, i * 7 FROM s;
    ";
    let db = TempDatabase::new_empty();
    let (_, hir) = connections(&db);
    for statement in SCHEMA.split(';').filter(|sql| !sql.trim().is_empty()) {
        hir.execute(statement).unwrap();
    }
    // A build input is stored only when the loops before the probe leave
    // few rows, and beats a temporary index on the probe when the probe is
    // large.
    for statement in [
        "ANALYZE",
        "DELETE FROM sqlite_stat1",
        "INSERT INTO sqlite_stat1(tbl, idx, stat) VALUES ('b', NULL, '100'), \
         ('c', NULL, '3000'), ('d', NULL, '1000000'), ('e', NULL, '3000')",
        "ANALYZE sqlite_schema",
    ] {
        hir.execute(statement).unwrap();
    }
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite.execute_batch(SCHEMA).unwrap();
    // (query, whether the chain's middle table gets a build input; the
    // other queries find a cheaper nested loop, as legacy does)
    for (sql, materializes) in [
        (
            "SELECT count(*), sum(b.id), sum(c.id), sum(d.id) FROM b \
             JOIN c ON b.k = c.k JOIN d ON d.k = c.k",
            true,
        ),
        (
            "SELECT b.k, count(*), sum(d.id) FROM b JOIN c ON b.k = c.k JOIN d ON d.k = c.k \
             GROUP BY b.k ORDER BY b.k",
            true,
        ),
        (
            "SELECT max(d.id), b.k, c.k FROM b JOIN c ON b.k = c.k JOIN d ON d.k = c.k",
            true,
        ),
        (
            "SELECT count(*), sum(b.id), sum(c.id), sum(d.id) FROM b \
             JOIN c ON b.k = c.k JOIN d ON d.k = c.k WHERE d.id > b.id + c.id",
            false,
        ),
        (
            "SELECT b.id, c.id, d.id FROM b JOIN c ON b.k = c.k JOIN d ON d.k = c.k \
             WHERE b.id < 20 ORDER BY 1, 2, 3",
            true,
        ),
        (
            "SELECT count(*) FROM b JOIN c ON b.k = c.k JOIN d ON d.k = c.k \
             WHERE EXISTS (SELECT 1 FROM e WHERE e.id = d.id AND e.v > b.id)",
            true,
        ),
        (
            "SELECT count(*), sum(b.id), sum(c.id), sum(d.id) FROM b \
             JOIN c ON b.k = c.k JOIN d ON d.k = c.k AND d.id > b.id",
            false,
        ),
        (
            "SELECT count(*), sum(e.v) FROM b JOIN c ON b.k = c.k JOIN d ON d.k = c.k \
             JOIN e ON e.id = b.id + d.id",
            false,
        ),
        // A LEFT JOIN among the loops that fill the build input.
        (
            "SELECT count(*), sum(b.id), count(e.id), sum(c.id), sum(d.id) FROM b \
             LEFT JOIN e ON e.id = b.id * 7, c JOIN d ON d.k = c.k \
             WHERE c.id % 5 = coalesce(e.v, 0) % 5 AND b.id % 3 = c.id % 3",
            true,
        ),
        // A LEFT JOIN probe, whose unmatched build rows come from the
        // temporary table, and one with a WHERE term on its NULL row.
        (
            "SELECT count(*), count(d.id), sum(b.id), sum(c.id) FROM b JOIN c ON b.k = c.k \
             LEFT JOIN d ON d.k = c.k AND d.id % 2 = 0",
            true,
        ),
        (
            "SELECT d.k, count(*), sum(b.id) FROM b JOIN c ON b.k = c.k \
             LEFT JOIN d ON d.k = c.k WHERE d.id IS NULL OR b.id % 7 = 0 \
             GROUP BY d.k ORDER BY d.k LIMIT 5",
            true,
        ),
        // A LEFT JOIN after the probe reads the hash row.
        (
            "SELECT count(*), sum(b.id), sum(c.id), sum(d.id), count(e.id) FROM b \
             JOIN c ON b.k = c.k JOIN d ON d.k = c.k LEFT JOIN e ON e.id = b.id + d.id",
            true,
        ),
        // A FULL JOIN never hashes.
        (
            "SELECT count(*), count(b.id), count(d.id), sum(b.id), sum(c.id), sum(d.id) \
             FROM b JOIN c ON b.k = c.k FULL JOIN d ON d.k = c.k",
            false,
        ),
    ] {
        let hir_plan = plan_without_ids(query_plan(&hir, sql));
        assert_eq!(
            hir_plan[0].starts_with("MATERIALIZE hash build input for "),
            materializes,
            "{sql}: {hir_plan:?}"
        );
        assert_eq!(hir_plan, plan_without_ids(expected_plan(sql)), "{sql}");
        let mut stmt = sqlite.prepare(sql).unwrap();
        let columns = stmt.column_count();
        let expected: Vec<Vec<rusqlite::types::Value>> = stmt
            .query_map([], |row| {
                (0..columns).map(|column| row.get(column)).collect()
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        // Legacy fills a bare column next to max() from the first row, not
        // from the row of the maximum, so only HIR's rows are compared.
        assert_eq!(limbo_exec_rows(&hir, sql), expected, "{sql}");
    }
}

/// Each EXPLAIN QUERY PLAN row legacy prints for a distinct aggregate, a
/// correlated IN probe, a compound ORDER BY and a shared CTE, HIR prints too,
/// in the same place. A CTE read twice is filled once before the loops: its
/// body rows stand on their own, `cte_materializations` points at them, and
/// both readers report a materialized reuse.
#[test]
fn hir_explain_rows_for_distinct_probes_compounds_and_shared_ctes() {
    let db = setup();
    let (_, hir) = connections(&db);
    for statement in [
        "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, city_id INT)",
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INT, amount INT)",
        "CREATE INDEX idx_orders_user ON orders(user_id)",
    ] {
        hir.execute(statement).unwrap();
    }
    const SHARED_CTE: &str = "WITH spenders AS (SELECT user_id, sum(amount) AS total \
         FROM orders GROUP BY user_id) \
         SELECT u.name, s1.total FROM users u \
         JOIN spenders s1 ON s1.user_id = u.id JOIN spenders s2 ON s2.user_id = u.city_id";
    for (sql, row) in [
        (
            "SELECT count(DISTINCT tag) FROM tags",
            "USE HASH TABLE FOR count(DISTINCT)",
        ),
        (
            "SELECT id FROM items WHERE value IN (SELECT item_id FROM tags WHERE item_id = items.id) \
             LIMIT 1",
            "CORRELATED LIST SUBQUERY",
        ),
        (
            "SELECT value FROM items UNION SELECT item_id FROM tags ORDER BY 1",
            "USE SORTER FOR ORDER BY",
        ),
        (SHARED_CTE, "SCAN orders USING INDEX idx_orders_user"),
    ] {
        let hir_plan = query_plan(&hir, sql);
        assert!(hir_plan.iter().any(|line| line.starts_with(row)), "{sql}: {hir_plan:?}");
        assert_eq!(
            plan_without_ids(hir_plan),
            plan_without_ids(expected_plan(sql)),
            "{sql}"
        );
    }

    let json = limbo_exec_rows(
        &hir,
        &format!("EXPLAIN QUERY PLAN FORMAT=JSON {SHARED_CTE}"),
    );
    let json = match json.as_slice() {
        [row] => match row.as_slice() {
            [rusqlite::types::Value::Text(json)] => json.clone(),
            _ => panic!("one JSON row expected: {json:?}"),
        },
        _ => panic!("one JSON row expected: {json:?}"),
    };
    assert_eq!(
        json.matches(r#""execution":"materialized_reuse","cte_id":0"#)
            .count(),
        2,
        "{json}"
    );
    let (_, materialization) = json
        .split_once(r#""cte_materializations":[{"cte_id":0,"name":"spenders","nodes":["#)
        .unwrap_or_else(|| panic!("{json}"));
    let body_id: u32 = materialization
        .split(']')
        .next()
        .and_then(|ids| ids.parse().ok())
        .unwrap_or_else(|| panic!("{json}"));
    assert!(
        json.contains(&format!(
            r#"{{"id":{body_id},"parent":null,"detail":"SCAN orders USING INDEX idx_orders_user""#
        )),
        "{json}"
    );
}

/// Legacy runs each window through a source subquery whose ORDER BY sorts
/// the rows, and EXPLAIN shows that sort. HIR has no such subquery but
/// prints the same sorter row for each window that sorts its input, the
/// innermost window first, and none for a window whose order the loops or
/// an inner window already give.
#[test]
fn hir_window_sorts_show_in_explain() {
    let db = setup();
    let (sqlite, hir) = connections(&db);
    for statement in [
        "CREATE TABLE sales(id INTEGER PRIMARY KEY, region TEXT, day INT, amount INT)",
        "CREATE INDEX sales_region ON sales(region)",
        "INSERT INTO sales VALUES (1, 'n', 3, 10), (2, 's', 1, 20), (3, 'n', 2, 30)",
    ] {
        hir.execute(statement).unwrap();
    }
    for sql in [
        "SELECT sum(amount) OVER (ORDER BY day) FROM sales",
        "SELECT sum(amount) OVER (ORDER BY day), max(amount) OVER (PARTITION BY region) FROM sales",
        "SELECT row_number() OVER (PARTITION BY region ORDER BY day), \
         sum(amount) OVER (PARTITION BY region), rank() OVER (ORDER BY amount DESC) FROM sales",
        // The index gives the partition order, so that window needs no sort.
        "SELECT count(*) OVER (PARTITION BY region) FROM sales",
        "SELECT region, sum(amount), row_number() OVER (ORDER BY sum(amount) DESC) \
         FROM sales GROUP BY region",
        "SELECT sum(amount) OVER (ORDER BY day) FROM sales ORDER BY day",
        "SELECT * FROM (SELECT amount, avg(amount) OVER (PARTITION BY region) AS a FROM sales) \
         WHERE a > 10 ORDER BY amount",
    ] {
        assert_eq!(
            plan_without_ids(query_plan(&hir, sql)),
            plan_without_ids(expected_plan(sql)),
            "{sql}"
        );
        assert_same_rows(&hir, &sqlite, sql);
    }
}

/// Like legacy and SQLite, with full_column_names on and short_column_names
/// off, `SELECT *` names each column after its source: the alias, the table
/// name, or `(subquery-N)` for a FROM subquery without an alias. Explicit
/// column references keep legacy's `table.column` rule, and other settings
/// keep the plain column name.
#[test]
fn hir_names_star_columns_after_their_source_under_long_names() {
    let db = setup();
    let (mut sqlite, hir) = connections(&db);
    for (full, short) in [(true, false), (true, true), (false, true), (false, false)] {
        hir.set_full_column_names(full);
        hir.set_short_column_names(short);
        sqlite.set_pragma("full_column_names", full);
        sqlite.set_pragma("short_column_names", short);
        for sql in [
            "SELECT * FROM items a, tags b LIMIT 1",
            "SELECT * FROM items, tags LIMIT 1",
            "SELECT a.*, b.tag FROM items a JOIN tags b ON b.item_id = a.id LIMIT 1",
            "SELECT * FROM items a, (SELECT 5, 6) LIMIT 1",
            "SELECT * FROM items a, (SELECT 5 AS n) AS s LIMIT 1",
            "SELECT a.id, value AS v, extra || 'x' FROM items a LIMIT 1",
            "SELECT * FROM (SELECT * FROM items a) LIMIT 1",
            "CREATE TABLE ctas AS SELECT * FROM items a LIMIT 1",
            "SELECT * FROM ctas",
            "DROP TABLE ctas",
        ] {
            // SQLite and HIR number unnamed subqueries differently.
            let without_ids = |names: Vec<String>| {
                names
                    .into_iter()
                    .map(|name| match name.strip_prefix("(subquery-") {
                        Some(rest) => {
                            format!("(subquery-{}", rest.trim_start_matches(char::is_numeric))
                        }
                        None => name,
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                without_ids(column_names(&hir, sql)),
                without_ids(sqlite.column_names(sql)),
                "{sql} full={full} short={short}"
            );
            if sql.starts_with("SELECT") {
                assert_same_rows(&hir, &sqlite, sql);
            } else {
                hir.execute(sql).unwrap();
            }
        }
    }
}

/// The EXPLAIN QUERY PLAN rows the planner is meant to choose, by query,
/// recorded when HIR matched the legacy planner on them.
const EXPECTED_PLANS: &[(&str, &[&str])] = &[
    (
        "SELECT id, (SELECT avg(i.amount) FROM i WHERE i.k1 = o.k1) FROM o ORDER BY id",
        &["SCAN o", "SEARCH scalar_subquery_1 USING INDEX ephemeral_subquery_s2 (correlation_key_0=?) LEFT-JOIN", "SCAN i", "USE SORTER FOR GROUP BY"],
    ),
    (
        "SELECT o.id FROM o WHERE o.amount < (SELECT avg(i.amount) FROM i WHERE i.k1 = o.k1)",
        &["SCAN o", "SEARCH scalar_subquery_1 USING INDEX ephemeral_subquery_s2 (correlation_key_0=?)", "SCAN i", "USE SORTER FOR GROUP BY"],
    ),
    (
        "SELECT count(*) FROM o WHERE o.x < (SELECT avg(i.q) FROM i WHERE i.k = o.k) AND o.x > (SELECT avg(i.q) FROM i WHERE i.k = o.k) - 100",
        &["SCAN o", "SEARCH scalar_subquery_1 USING INDEX ephemeral_subquery_s3 (correlation_key_0=?)", "SCAN i", "USE SORTER FOR GROUP BY"],
    ),
    (
        "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1) AND EXISTS (SELECT 1 FROM j WHERE j.k2 = o.k2)",
        &["SCAN o", "SEARCH i USING COVERING INDEX ephemeral_i_s1 (k1=?)", "SEARCH j USING COVERING INDEX ephemeral_j_s2 (k2=?)"],
    ),
    (
        "SELECT o.id FROM o WHERE o.body LIKE 'apple%' AND EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1)",
        &["SCAN o", "SEARCH i USING COVERING INDEX ephemeral_i_s1 (k1=?)"],
    ),
    (
        "SELECT o.id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1)",
        &["HASH JOIN i", "SCAN o"],
    ),
    (
        "SELECT o.id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1) AND NOT EXISTS (SELECT 1 FROM j WHERE j.k2 = o.k2)",
        &["HASH JOIN i", "SEARCH j USING COVERING INDEX ephemeral_j_s2 (k2=?)", "SCAN o"],
    ),
    (
        "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1) AND NOT EXISTS (SELECT 1 FROM j WHERE j.k2 = o.k2)",
        &["SCAN o", "SEARCH i USING COVERING INDEX ephemeral_i_s1 (k1=?)", "SEARCH j USING COVERING INDEX ephemeral_j_s2 (k2=?)"],
    ),
    (
        "SELECT o.id FROM o WHERE o.k2 IN (SELECT j.k2 FROM j WHERE j.k2 = o.k1)",
        &["SCAN o", "SEARCH j USING COVERING INDEX ephemeral_j_s1 (k2=?)"],
    ),
    (
        "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM p WHERE p.id = o.k1)",
        &["SCAN o", "SEARCH p USING INTEGER PRIMARY KEY (rowid=?)"],
    ),
    (
        "SELECT o.id, count(*) FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1) GROUP BY o.id",
        &["SCAN o", "SEARCH i USING COVERING INDEX ephemeral_i_s1 (k1=?)"],
    ),
    (
        "SELECT o.id, p.id FROM o LEFT JOIN p ON p.k = o.k1 WHERE EXISTS (SELECT 1 FROM i WHERE i.k1 = p.id)",
        &["SCAN o", "SEARCH p USING COVERING INDEX p_k (k=?) LEFT-JOIN", "SCAN i"],
    ),
    (
        "SELECT o.id FROM o WHERE o.k2 NOT IN (SELECT j.k2 FROM j WHERE j.k2 = o.k1)",
        &["SCAN o", "CORRELATED LIST SUBQUERY 1", "SCAN j"],
    ),
    (
        "SELECT o.id FROM o WHERE o.k1 = 2 OR EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1)",
        &["SCAN o", "SCAN i"],
    ),
    (
        "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k1 < o.k1)",
        &["SCAN o", "SEARCH i USING COVERING INDEX ephemeral_i_s1 (k1<?)"],
    ),
    (
        "SELECT o.id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.k1 = o.k1 AND o.k2 = 1)",
        &["SCAN o", "SCAN i"],
    ),
    (
        "SELECT count(*) FROM o WHERE o.v < (SELECT avg(i.v) FROM i WHERE i.k = o.k)",
        &["SCAN o", "SEARCH scalar_subquery_1 USING INDEX ephemeral_subquery_s2 (correlation_key_0=?)", "SCAN i", "USE SORTER FOR GROUP BY"],
    ),
    (
        "SELECT a.k, count(*), sum(b.v) FROM a JOIN b ON b.k = a.k GROUP BY a.k ORDER BY a.k",
        &["HASH JOIN b", "SCAN a", "USE SORTER FOR GROUP BY"],
    ),
    (
        "SELECT a.id, b.id FROM a JOIN b ON b.k = a.k WHERE a.id < 30 ORDER BY a.id, b.id",
        &["MATERIALIZE hash build input for a", "SEARCH a USING INTEGER PRIMARY KEY (rowid=?)", "HASH JOIN b", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT count(*), count(b.id), sum(b.v) FROM a LEFT JOIN b ON b.k = a.k",
        &["HASH JOIN b", "SCAN a"],
    ),
    (
        "SELECT count(*), count(b.id) FROM a LEFT JOIN b ON b.k = a.k AND b.v <> 3",
        &["HASH JOIN b", "SCAN a"],
    ),
    (
        "SELECT count(*), sum(a.id) FROM a LEFT JOIN b ON b.k = a.k WHERE b.id IS NULL",
        &["HASH JOIN b", "SCAN a"],
    ),
    (
        "SELECT a.id, b.id FROM a LEFT JOIN b ON b.k = a.k + 45 WHERE a.id < 20 ORDER BY a.id, b.id",
        &["MATERIALIZE hash build input for a", "SEARCH a USING INTEGER PRIMARY KEY (rowid=?)", "HASH JOIN b", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT count(*), count(b.id) FROM a LEFT JOIN b ON b.v = a.k + 100",
        &["HASH JOIN b", "SCAN a"],
    ),
    (
        "SELECT count(*), count(a.id), count(b.id), sum(b.v) FROM a FULL JOIN b ON b.k = a.k",
        &["SCAN a", "SCAN b LEFT-JOIN", "RIGHT-JOIN b", "SCAN b"],
    ),
    (
        "SELECT count(*), count(a.id), count(b.id) FROM a FULL JOIN b ON b.k = a.k WHERE a.id IS NULL OR b.v > 10",
        &["SCAN a", "SCAN b LEFT-JOIN", "RIGHT-JOIN b", "SCAN b"],
    ),
    (
        "SELECT sum((SELECT count(b.id) FROM a a2 JOIN b ON b.k = a2.k WHERE a2.id = a.id)) FROM a WHERE a.id < 300",
        &["SEARCH a USING INTEGER PRIMARY KEY (rowid=?)", "SEARCH scalar_subquery_1 USING INDEX ephemeral_subquery_s3 (correlation_key_0=?) LEFT-JOIN", "HASH JOIN b", "SCAN a AS a2", "USE SORTER FOR GROUP BY"],
    ),
    (
        "SELECT count(*), sum(s.n) FROM a, (SELECT a2.id AS i, b.id AS n FROM a a2 JOIN b ON b.k = a2.k WHERE a2.id < 40) s WHERE a.id < 4 AND s.i = a.id + 3",
        &["SEARCH a USING INTEGER PRIMARY KEY (rowid=?)", "SEARCH s USING INDEX ephemeral_subquery_s3 (i=?)", "MATERIALIZE hash build input for a AS a2", "SEARCH a2 USING INTEGER PRIMARY KEY (rowid=?)", "HASH JOIN b"],
    ),
    (
        "SELECT c.name, o.order_id, p.name, oi.quantity FROM customers c JOIN orders o ON c.customer_id = o.customer_id JOIN order_items oi ON o.order_id = oi.order_id JOIN products p ON oi.product_id = p.product_id",
        &["SCAN order_items AS oi", "SEARCH p USING INTEGER PRIMARY KEY (rowid=?)", "SEARCH o USING INTEGER PRIMARY KEY (rowid=?)", "SEARCH c USING INTEGER PRIMARY KEY (rowid=?)"],
    ),
    (
        "SELECT id FROM items WHERE value IN (SELECT item_id FROM tags WHERE item_id = items.id) LIMIT 1",
        &["SCAN items USING COVERING INDEX items_value", "CORRELATED LIST SUBQUERY 1", "SCAN tags"],
    ),
    (
        "SELECT value FROM items UNION SELECT item_id FROM tags ORDER BY 1",
        &["COMPOUND QUERY", "LEFT-MOST SUBQUERY", "SCAN items USING COVERING INDEX items_value", "UNION USING TEMP B-TREE", "SCAN tags", "USE SORTER FOR ORDER BY"],
    ),
    (
        "WITH spenders AS (SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id) SELECT u.name, s1.total FROM users u JOIN spenders s1 ON s1.user_id = u.id JOIN spenders s2 ON s2.user_id = u.city_id",
        &["SCAN orders USING INDEX idx_orders_user", "SCAN users AS u", "SEARCH s1 USING INDEX ephemeral_subquery_s2 (user_id=?)", "SEARCH s2 USING INDEX ephemeral_subquery_s3 (user_id=?)"],
    ),
    (
        "SELECT a.id, l.id, r.id FROM l AS a, l FULL JOIN r ON l.k = r.k WHERE a.id = 1 ORDER BY 1, 2, 3",
        &["SEARCH a USING INTEGER PRIMARY KEY (rowid=?)", "HASH JOIN r", "SCAN l", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT extra, value, count(*) FROM items GROUP BY extra, value ORDER BY extra, value",
        &["SCAN items USING COVERING INDEX items_extra_value"],
    ),
    (
        "SELECT extra, value, count(*) FROM items GROUP BY extra, value ORDER BY extra DESC, value DESC",
        &["SCAN items USING COVERING INDEX items_extra_value"],
    ),
    (
        "SELECT extra, value, count(*) FROM items GROUP BY value, extra ORDER BY extra",
        &["SCAN items USING COVERING INDEX items_extra_value"],
    ),
    (
        "SELECT extra, value, count(*) FROM items GROUP BY extra, value ORDER BY value",
        &["SCAN items", "USE SORTER FOR GROUP BY"],
    ),
    (
        "SELECT extra, value, count(*) FROM items GROUP BY extra, value ORDER BY extra DESC NULLS FIRST, value",
        &["SCAN items", "USE SORTER FOR GROUP BY"],
    ),
    (
        "SELECT extra, max(value) FROM items GROUP BY extra ORDER BY extra",
        &["SCAN items USING COVERING INDEX items_extra_value"],
    ),
    (
        "SELECT h.order_id, i.product FROM h_orders h LEFT JOIN h_items i ON h.customer_name = i.order_name ORDER BY 1, 2",
        &["HASH JOIN h_items AS i", "SCAN h_orders AS h", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT h.order_id, i.item_id FROM h_orders h FULL JOIN h_items i ON h.customer_name = i.order_name ORDER BY 1, 2",
        &["SCAN h_orders AS h", "SCAN h_items AS i LEFT-JOIN", "RIGHT-JOIN i", "SCAN h_items AS i", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT h.customer_name, count(i.item_id) FROM h_orders h LEFT JOIN h_items i ON h.customer_name = i.order_name GROUP BY h.customer_name ORDER BY 1",
        &["HASH JOIN h_items AS i", "SCAN h_orders AS h", "USE SORTER FOR GROUP BY"],
    ),
    (
        "SELECT h.order_id, i.item_id FROM h_orders h JOIN h_items i ON i.quantity = h.amount / 10 ORDER BY 1, 2",
        &["HASH JOIN h_items AS i", "SCAN h_orders AS h", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT b.k, count(*), sum(d.id) FROM b JOIN c ON b.k = c.k JOIN d ON d.k = c.k GROUP BY b.k ORDER BY b.k",
        &["MATERIALIZE hash build input for c", "HASH JOIN c", "SCAN b", "HASH JOIN d", "USE SORTER FOR GROUP BY"],
    ),
    (
        "SELECT max(d.id), b.k, c.k FROM b JOIN c ON b.k = c.k JOIN d ON d.k = c.k",
        &["MATERIALIZE hash build input for c", "HASH JOIN c", "SCAN b", "HASH JOIN d"],
    ),
    (
        "SELECT count(*), sum(b.id), sum(c.id), sum(d.id) FROM b JOIN c ON b.k = c.k JOIN d ON d.k = c.k WHERE d.id > b.id + c.id",
        &["SCAN d", "SEARCH c USING COVERING INDEX ephemeral_c_s1 (k=?)", "SEARCH b USING COVERING INDEX ephemeral_b_s0 (k=?)"],
    ),
    (
        "SELECT b.id, c.id, d.id FROM b JOIN c ON b.k = c.k JOIN d ON d.k = c.k WHERE b.id < 20 ORDER BY 1, 2, 3",
        &["MATERIALIZE hash build input for c", "HASH JOIN c", "SCAN b", "HASH JOIN d", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT count(*) FROM b JOIN c ON b.k = c.k JOIN d ON d.k = c.k WHERE EXISTS (SELECT 1 FROM e WHERE e.id = d.id AND e.v > b.id)",
        &["MATERIALIZE hash build input for c", "HASH JOIN c", "SCAN b", "HASH JOIN d", "SEARCH e USING INTEGER PRIMARY KEY (rowid=?)"],
    ),
    (
        "SELECT count(*), sum(b.id), sum(c.id), sum(d.id) FROM b JOIN c ON b.k = c.k JOIN d ON d.k = c.k AND d.id > b.id",
        &["SCAN b", "SEARCH d USING INTEGER PRIMARY KEY (rowid=?)", "SEARCH c USING COVERING INDEX ephemeral_c_s1 (k=?)"],
    ),
    (
        "SELECT count(*), sum(e.v) FROM b JOIN c ON b.k = c.k JOIN d ON d.k = c.k JOIN e ON e.id = b.id + d.id",
        &["SCAN b", "SCAN d", "SEARCH e USING INTEGER PRIMARY KEY (rowid=?)", "SEARCH c USING COVERING INDEX ephemeral_c_s1 (k=?)"],
    ),
    (
        "SELECT count(*), sum(b.id), count(e.id), sum(c.id), sum(d.id) FROM b LEFT JOIN e ON e.id = b.id * 7, c JOIN d ON d.k = c.k WHERE c.id % 5 = coalesce(e.v, 0) % 5 AND b.id % 3 = c.id % 3",
        &["MATERIALIZE hash build input for c", "SCAN b", "SEARCH e USING INTEGER PRIMARY KEY (rowid=?) LEFT-JOIN", "SCAN c", "HASH JOIN d"],
    ),
    (
        "SELECT count(*), count(d.id), sum(b.id), sum(c.id) FROM b JOIN c ON b.k = c.k LEFT JOIN d ON d.k = c.k AND d.id % 2 = 0",
        &["MATERIALIZE hash build input for c", "HASH JOIN c", "SCAN b", "HASH JOIN d"],
    ),
    (
        "SELECT d.k, count(*), sum(b.id) FROM b JOIN c ON b.k = c.k LEFT JOIN d ON d.k = c.k WHERE d.id IS NULL OR b.id % 7 = 0 GROUP BY d.k ORDER BY d.k LIMIT 5",
        &["MATERIALIZE hash build input for c", "HASH JOIN c", "SCAN b", "HASH JOIN d", "USE SORTER FOR GROUP BY"],
    ),
    (
        "SELECT count(*), sum(b.id), sum(c.id), sum(d.id), count(e.id) FROM b JOIN c ON b.k = c.k JOIN d ON d.k = c.k LEFT JOIN e ON e.id = b.id + d.id",
        &["MATERIALIZE hash build input for c", "HASH JOIN c", "SCAN b", "HASH JOIN d", "SEARCH e USING INTEGER PRIMARY KEY (rowid=?) LEFT-JOIN"],
    ),
    (
        "SELECT count(*), count(b.id), count(d.id), sum(b.id), sum(c.id), sum(d.id) FROM b JOIN c ON b.k = c.k FULL JOIN d ON d.k = c.k",
        &["SCAN b", "SCAN c", "SCAN d LEFT-JOIN", "RIGHT-JOIN d", "SCAN d"],
    ),
    (
        "SELECT count(*), sum(a.id), sum(b.id), sum(c.id) FROM a, b LEFT JOIN c ON b.k = c.k",
        &["SCAN a", "HASH JOIN c", "SCAN b"],
    ),
    (
        "SELECT count(*), sum(a.id), sum(b.id), sum(c.id) FROM a, b LEFT JOIN c ON b.k = c.k AND c.id % 3 = a.id - 1 WHERE c.id IS NULL OR a.id = 2",
        &["SCAN a", "HASH JOIN c", "SCAN b"],
    ),
    (
        "SELECT count(*), sum(a.id), sum(b.id), sum(c.id), sum(d.id) FROM a LEFT JOIN d ON d.id = a.x * 1000, b LEFT JOIN c ON b.k = c.k",
        &["SCAN a", "SEARCH d USING INTEGER PRIMARY KEY (rowid=?) LEFT-JOIN", "HASH JOIN c", "SCAN b"],
    ),
    (
        "SELECT a.id, (SELECT count(*) FROM b LEFT JOIN c ON b.k = c.k AND c.id % 3 = a.id - 1) FROM a ORDER BY a.id",
        &["SCAN a", "CORRELATED SCALAR SUBQUERY 1", "HASH JOIN c", "SCAN b"],
    ),
    (
        "SELECT count(*), sum(a.id), sum(b.id), sum(c.id) FROM a, b FULL JOIN c ON b.k = c.k",
        &["SCAN a", "SCAN b", "SEARCH c USING COVERING INDEX ephemeral_c_s2 (k=?) LEFT-JOIN"],
    ),
    (
        "SELECT id FROM items WHERE extra IS NULL AND value IS NULL",
        &["SEARCH items USING COVERING INDEX items_extra_id (extra=? AND value=?)"],
    ),
    (
        "SELECT id FROM items WHERE extra IS NULL AND value > 10 ORDER BY id",
        &["SCAN items"],
    ),
    (
        "SELECT id FROM items WHERE lower(extra) IS NULL ORDER BY id",
        &["SEARCH items USING INDEX items_lower_extra (lower (extra)=?)"],
    ),
    (
        "SELECT tags.tag, items.id FROM tags LEFT JOIN items ON items.id = tags.item_id WHERE items.extra IS NULL ORDER BY 1",
        &["SCAN tags", "SEARCH items USING INTEGER PRIMARY KEY (rowid=?) LEFT-JOIN", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT tags.tag, items.id FROM tags LEFT JOIN items ON items.extra IS NULL AND items.id = tags.item_id ORDER BY 1",
        &["SCAN tags", "SEARCH items USING INTEGER PRIMARY KEY (rowid=?) LEFT-JOIN", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT id FROM items WHERE extra IS NULL",
        &["SEARCH items USING COVERING INDEX items_extra_id (extra=?)"],
    ),
    (
        "SELECT id FROM items WHERE extra ISNULL",
        &["SEARCH items USING COVERING INDEX items_extra_id (extra=?)"],
    ),
    (
        "WITH g AS (SELECT extra, count(*) AS n FROM items GROUP BY extra) SELECT * FROM g ORDER BY extra",
        &["SCAN g", "SCAN items", "USE SORTER FOR GROUP BY"],
    ),
    (
        "WITH a AS (SELECT value, extra FROM items ORDER BY value), b AS (SELECT value, extra FROM a) SELECT * FROM b ORDER BY value",
        &["SCAN b", "SCAN a", "SCAN items USING INDEX items_value"],
    ),
    (
        "SELECT * FROM (SELECT value, extra FROM items ORDER BY value DESC NULLS FIRST) ORDER BY value DESC NULLS FIRST",
        &["SCAN (subquery-0)", "SCAN items", "USE SORTER FOR ORDER BY"],
    ),
    (
        "WITH o AS (SELECT value, extra FROM items ORDER BY value) SELECT * FROM o ORDER BY value DESC",
        &["SCAN o", "SCAN items USING INDEX items_value", "USE SORTER FOR ORDER BY"],
    ),
    (
        "WITH o AS (SELECT value, extra FROM items ORDER BY value) SELECT * FROM o ORDER BY value, extra",
        &["SCAN o", "SCAN items USING INDEX items_value", "USE SORTER FOR ORDER BY"],
    ),
    (
        "WITH o AS (SELECT value, extra FROM items ORDER BY extra) SELECT * FROM o ORDER BY extra",
        &["SCAN o", "SCAN items", "USE SORTER FOR ORDER BY"],
    ),
    (
        "WITH o AS MATERIALIZED (SELECT value, extra FROM items ORDER BY extra DESC, value DESC) SELECT * FROM o ORDER BY extra, value",
        &["SCAN items", "USE SORTER FOR ORDER BY", "SCAN o"],
    ),
    (
        "WITH o AS MATERIALIZED (SELECT value, extra FROM items ORDER BY extra, value DESC) SELECT * FROM o ORDER BY extra DESC, value",
        &["SCAN items", "USE SORTER FOR ORDER BY", "SCAN o"],
    ),
    (
        "WITH g AS MATERIALIZED (SELECT extra, sum(value) AS s FROM items GROUP BY extra) SELECT * FROM g ORDER BY extra DESC",
        &["SCAN items", "USE SORTER FOR GROUP BY", "SCAN g"],
    ),
    (
        "WITH o AS MATERIALIZED (SELECT value, extra FROM items ORDER BY extra, value DESC) SELECT * FROM o ORDER BY extra, value",
        &["SCAN items", "USE SORTER FOR ORDER BY", "SCAN o", "USE SORTER FOR ORDER BY"],
    ),
    (
        "WITH o AS (SELECT value, extra FROM items ORDER BY value LIMIT 3) SELECT * FROM o ORDER BY value",
        &["SCAN o", "SCAN items USING INDEX items_value"],
    ),
    (
        "SELECT count(*), sum(b.id), sum(c.id) FROM b FULL JOIN c ON b.k = c.k",
        &["SCAN b", "SCAN c LEFT-JOIN", "RIGHT-JOIN c", "SCAN c"],
    ),
    (
        "SELECT o.id, p.id FROM o FULL JOIN p ON p.k = o.k WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.k = p.k) ORDER BY 1, 2",
        &["SCAN o", "SCAN p LEFT-JOIN", "SEARCH i USING COVERING INDEX ephemeral_i_s2 (k=?)", "RIGHT-JOIN p", "SCAN p", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT o.id, p.id FROM o RIGHT JOIN p ON p.k = o.k WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.k = p.k) ORDER BY 1, 2",
        &["SCAN o", "SEARCH p USING COVERING INDEX ephemeral_p_s1 (k=?)", "SEARCH i USING COVERING INDEX ephemeral_i_s2 (k=?)", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT o.id, p.id FROM o RIGHT JOIN p ON p.k = o.k WHERE p.k IN (SELECT i.k FROM i WHERE i.k = p.id) ORDER BY 1, 2",
        &["SCAN o", "SCAN p", "SEARCH i USING COVERING INDEX ephemeral_i_s2 (k=?)", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT o.id FROM o WHERE EXISTS (SELECT (SELECT max(x) FROM d) FROM i WHERE i.k = o.k) ORDER BY 1",
        &["SCAN o", "SEARCH i USING COVERING INDEX ephemeral_i_s1 (k=?)"],
    ),
    (
        "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM generate_series(1, o.k) g WHERE g.value = 2) ORDER BY 1",
        &["SCAN o", "SCAN generate_series AS g"],
    ),
    (
        "SELECT o.id FROM o WHERE o.k IS NOT NULL AND NOT EXISTS (SELECT 1 FROM generate_series(o.k, 2) g WHERE g.value = o.k) ORDER BY 1",
        &["SCAN o", "SCAN generate_series AS g"],
    ),
    (
        "SELECT o.id, p.id FROM o FULL JOIN p ON p.k = o.k WHERE EXISTS (SELECT 1 FROM i WHERE i.k = o.k) ORDER BY 1, 2",
        &["SCAN o", "SCAN p LEFT-JOIN", "SEARCH i USING COVERING INDEX ephemeral_i_s2 (k=?)", "RIGHT-JOIN p", "SCAN p", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT o.id FROM o WHERE EXISTS (SELECT ?1 FROM i WHERE i.k = o.k) AND o.v > ?2 ORDER BY 1",
        &["SCAN o", "SEARCH i USING COVERING INDEX ephemeral_i_s1 (k=?)"],
    ),
    (
        "SELECT c.name, s.total FROM customers c JOIN (SELECT customer_id, sum(total) AS total FROM orders GROUP BY customer_id) AS s ON s.customer_id = c.id WHERE s.total > 500 ORDER BY 1",
        &["SCAN customers AS c", "SEARCH s USING INDEX ephemeral_subquery_s2 (customer_id=? AND total>?)", "SCAN orders", "USE SORTER FOR GROUP BY", "USE SORTER FOR ORDER BY"],
    ),
    (
        "WITH sorted AS (SELECT DISTINCT v FROM points ORDER BY v) SELECT p1.v, (SELECT max(p2.v) FROM sorted p2 WHERE p2.v < p1.v) FROM sorted p1 ORDER BY 1",
        &["SCAN points", "USE HASH TABLE FOR DISTINCT", "USE SORTER FOR ORDER BY", "SCAN sorted AS p1", "CORRELATED SCALAR SUBQUERY 1", "SEARCH p2 USING INDEX ephemeral_subquery_s2 (v<?)", "USE HASH TABLE FOR DISTINCT", "USE SORTER FOR ORDER BY"],
    ),
    (
        "WITH t AS (SELECT cust_id, sum(total) AS s FROM ord GROUP BY cust_id) SELECT c.name, t.s FROM cust c JOIN t ON t.cust_id = c.id WHERE t.s > (SELECT avg(s) FROM t) ORDER BY 1",
        &["SCAN ord USING INDEX ord_cust", "SCAN cust AS c", "SEARCH t USING INDEX ephemeral_subquery_s2 (cust_id=? AND s>?)", "SCALAR SUBQUERY 1", "SCAN t", "USE SORTER FOR ORDER BY"],
    ),
    (
        "WITH p(price) AS MATERIALIZED (SELECT price FROM prod) SELECT p1.price, (SELECT max(p2.price) FROM p p2 WHERE p2.price < p1.price) FROM p p1 WHERE p1.price < 100",
        &["SCAN prod USING COVERING INDEX prod_price", "SCAN p AS p1", "CORRELATED SCALAR SUBQUERY 1", "SCAN p AS p2"],
    ),
    (
        "SELECT sum(amount) OVER (ORDER BY day), max(amount) OVER (PARTITION BY region) FROM sales",
        &["SCAN sales USING INDEX sales_region", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT row_number() OVER (PARTITION BY region ORDER BY day), sum(amount) OVER (PARTITION BY region), rank() OVER (ORDER BY amount DESC) FROM sales",
        &["SCAN sales", "USE SORTER FOR ORDER BY", "USE SORTER FOR ORDER BY", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT count(*) OVER (PARTITION BY region) FROM sales",
        &["SCAN sales USING COVERING INDEX sales_region"],
    ),
    (
        "SELECT region, sum(amount), row_number() OVER (ORDER BY sum(amount) DESC) FROM sales GROUP BY region",
        &["SCAN sales USING INDEX sales_region", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT sum(amount) OVER (ORDER BY day) FROM sales ORDER BY day",
        &["SCAN sales", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT * FROM (SELECT amount, avg(amount) OVER (PARTITION BY region) AS a FROM sales) WHERE a > 10 ORDER BY amount",
        &["SCAN (subquery-0)", "SCAN sales USING INDEX sales_region", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT id, extra, sum(value) OVER (PARTITION BY extra ORDER BY value) FROM items",
        &["SCAN items USING COVERING INDEX items_extra_value"],
    ),
    (
        "SELECT id, rank() OVER (PARTITION BY extra ORDER BY value DESC) FROM items",
        &["SCAN items", "USE SORTER FOR ORDER BY"],
    ),
    (
        "WITH o AS (SELECT id, value FROM items ORDER BY value) SELECT id, sum(value) OVER (ORDER BY value) FROM o",
        &["SCAN o", "SCAN items USING COVERING INDEX items_value"],
    ),
    (
        "WITH o AS (SELECT id, extra FROM items NOT INDEXED ORDER BY extra) SELECT id, count(*) OVER (ORDER BY extra) FROM o",
        &["SCAN o", "SCAN items", "USE SORTER FOR ORDER BY"],
    ),
    (
        "SELECT id, row_number() OVER (ORDER BY value) FROM items LIMIT 3",
        &["SCAN items USING COVERING INDEX items_value"],
    ),
    (
        "WITH o AS (SELECT value, extra FROM items ORDER BY value) SELECT * FROM o ORDER BY value",
        &["SCAN o", "SCAN items USING INDEX items_value"],
    ),
];

#[test]
fn cte_read_by_a_correlated_max_is_filled_once_and_searched() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE days(day INT)").unwrap();
    conn.execute("INSERT INTO days VALUES (4), (1), (2), (2)")
        .unwrap();
    let sql = "WITH dates AS (SELECT DISTINCT day FROM days ORDER BY day) \
         SELECT d1.day, (SELECT max(d2.day) FROM dates d2 WHERE d2.day < d1.day) \
         FROM dates d1";
    let table_reads = limbo_exec_rows(&conn, &format!("EXPLAIN {sql}"))
        .into_iter()
        .filter(|row| row[1] == rusqlite::types::Value::Text("OpenRead".into()))
        .count();
    assert_eq!(table_reads, 1);
    let plan = query_plan(&conn, sql);
    assert!(
        plan.iter()
            .any(|line| line.starts_with("SEARCH d2 USING INDEX")),
        "{plan:?}"
    );
    use rusqlite::types::Value::{Integer, Null};
    assert_eq!(
        limbo_exec_rows(&conn, sql),
        vec![
            vec![Integer(1), Null],
            vec![Integer(2), Integer(1)],
            vec![Integer(4), Integer(2)],
        ]
    );
}
