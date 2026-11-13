//! Every query here compares, sorts or hashes only values of a type with its
//! own order. No instruction of its program may use a text collation: if one
//! does, some code path took the collation from the text rules instead of
//! from the value's type, and would compare '10.00' before '9.50'.

#[allow(unused_imports)]
use std::str::FromStr as _;
use turso_core::security::roles::SessionRole;
use turso_core::sync::Arc;
use turso_core::translate::collate::CollationSeq;
use turso_core::vdbe::{builder::CursorType, insn::Insn};
use turso_core::{ConnectOpts, Connection, Database, DatabaseOpts, MemoryIO, OpenFlags, IO};
use turso_sqlite::SqliteDialect;

/// Each query, with the number of its instructions that compare values the
/// engine made itself (row counters, sequence numbers) with BINARY.
const QUERIES: &[(&str, usize)] = &[
    // comparisons
    ("SELECT n FROM t WHERE n > 1.5", 0),
    ("SELECT n FROM t WHERE n BETWEEN 1.5 AND 2", 0),
    ("SELECT n FROM t WHERE a = '{1}'", 0),
    ("SELECT n FROM t WHERE b < '{x}'", 0),
    ("SELECT n FROM t WHERE n = (SELECT max(m) FROM u)", 0),
    ("SELECT n > 1.5, nullif(n, 2), n IS 1.5 FROM t", 0),
    // IN
    ("SELECT n FROM t WHERE n IN (1.5, 2)", 0),
    ("SELECT n FROM t WHERE n NOT IN (1.5, 2)", 0),
    ("SELECT n FROM t WHERE n IN (SELECT m FROM u)", 0),
    (
        "SELECT n FROM t WHERE n IN (SELECT m FROM u UNION SELECT n FROM t)",
        0,
    ),
    ("SELECT n FROM t WHERE a IN (SELECT a FROM t)", 0),
    // CASE
    ("SELECT CASE n WHEN 1.5 THEN 1 WHEN 2 THEN 2 END FROM t", 0),
    (
        "SELECT n FROM t ORDER BY CASE WHEN a IS NULL THEN n ELSE n END",
        0,
    ),
    // ORDER BY
    ("SELECT n FROM t ORDER BY n", 0),
    ("SELECT n FROM t ORDER BY a DESC, b", 0),
    ("SELECT n FROM t ORDER BY likely(n), coalesce(n, 1)", 0),
    ("SELECT n + 1 AS x FROM t ORDER BY x", 0),
    // GROUP BY
    ("SELECT n FROM t GROUP BY n", 0),
    ("SELECT a, b FROM t GROUP BY a, b", 0),
    ("SELECT n FROM t GROUP BY n HAVING n > 1", 0),
    // DISTINCT
    ("SELECT DISTINCT n FROM t", 0),
    ("SELECT DISTINCT a, b FROM t", 0),
    ("SELECT count(DISTINCT n) FROM t", 0),
    // compound
    ("SELECT n FROM t UNION SELECT m FROM u", 0),
    ("SELECT n FROM t INTERSECT SELECT m FROM u", 0),
    ("SELECT n FROM t EXCEPT SELECT m FROM u", 0),
    // The sorter's last key is the sequence number of the row.
    ("SELECT n FROM t UNION ALL SELECT m FROM u ORDER BY 1", 1),
    ("SELECT a FROM t UNION SELECT a FROM t", 0),
    // window
    ("SELECT min(n) OVER (PARTITION BY a ORDER BY b) FROM t", 0),
    (
        "SELECT max(n) OVER (ORDER BY n RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        0,
    ),
    ("SELECT n FROM t ORDER BY lag(n) OVER (ORDER BY a)", 0),
    // Two comparisons of the frame's row numbers.
    (
        "SELECT n FROM t ORDER BY first_value(b) OVER (ORDER BY n)",
        2,
    ),
    // min/max
    ("SELECT min(n), max(a), min(b) FROM t", 0),
    ("SELECT max(n, 2), min(n, 1.5) FROM t", 0),
    // join
    ("SELECT t.n FROM t JOIN u ON t.n = u.m", 0),
    ("SELECT t.n FROM t LEFT JOIN u ON t.n = u.m", 0),
    ("SELECT t.n FROM t, u WHERE t.n > u.m", 0),
    (
        "SELECT t.n FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.m = t.n)",
        0,
    ),
    // A value without a type compared with typed values.
    ("SELECT n FROM t WHERE 1.5 IN (n, 2)", 0),
    ("SELECT n FROM t WHERE '{1}' IN (a)", 0),
    // Registers that stand for columns.
    (
        "INSERT INTO w VALUES (1, '{1}', 1.5) ON CONFLICT(k) DO UPDATE \
         SET a = excluded.a WHERE excluded.a > w.a AND excluded.n > w.n",
        0,
    ),
    ("UPDATE w SET a = '{2}'", 0),
    (
        "INSERT INTO w VALUES (2, '{1}', 1.5) RETURNING a > '{0}', n > 1.5",
        0,
    ),
    // Array function results.
    (
        "SELECT n FROM t WHERE b = ARRAY['9'] OR array_append(a, 1) > a",
        0,
    ),
    ("SELECT n FROM t WHERE string_to_array('9', ',') = b", 0),
    // A compound subquery in FROM.
    (
        "SELECT x FROM (SELECT n AS x FROM t UNION ALL SELECT m FROM u) WHERE x > 1.5",
        0,
    ),
    (
        "SELECT DISTINCT x FROM (SELECT 1 AS x UNION ALL SELECT n FROM t)",
        0,
    ),
    // Automatic indexes.
    ("SELECT t.n FROM t, u WHERE t.n = u.m AND u.m > 1", 0),
];

#[test]
fn no_text_collation_on_typed_values() {
    let conn = connection_with_typed_tables();
    let mut failures = Vec::new();
    for &(sql, internal_comparisons) in QUERIES {
        let stmt = conn.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let program = &stmt.get_program().prepared;
        let instructions = program
            .insns
            .iter()
            .filter(|(insn, _)| compares_values(insn))
            .flat_map(|(insn, _)| {
                insn.collations()
                    .into_iter()
                    .map(move |collation| (format!("{insn:?}"), collation))
            });
        let index_keys = program
            .cursor_ref
            .iter()
            .filter_map(|(_, cursor_type)| match cursor_type {
                CursorType::BTreeIndex(index) if index.ephemeral => Some(index),
                _ => None,
            })
            .flat_map(|index| {
                index.columns.iter().map(move |column| {
                    (
                        format!("key {} of index {}", column.name, index.name),
                        column.collation,
                    )
                })
            });
        let with_text_collation: Vec<String> = instructions
            .chain(index_keys)
            .filter(|(_, collation)| matches!(collation, CollationSeq::Text(_)))
            .map(|(place, _)| place)
            .collect();
        if with_text_collation.len() != internal_comparisons {
            failures.push(format!("{sql}\n    {}", with_text_collation.join("\n    ")));
        }
    }
    assert!(
        failures.is_empty(),
        "comparisons of typed values that use a text collation:\n{}",
        failures.join("\n")
    );
}

/// Functions and aggregates that never compare values carry an unused
/// BINARY collation.
fn compares_values(insn: &Insn) -> bool {
    match insn {
        Insn::Function { func, .. } => func.func.needs_collation(),
        Insn::AggStep { data } => data.func.compares_values(),
        _ => true,
    }
}

fn connection_with_typed_tables() -> Arc<Connection> {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        DatabaseOpts::new().with_custom_types(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t(n numeric(10,2), a integer[], b text[]) STRICT")
        .unwrap();
    conn.execute("CREATE TABLE u(m numeric(10,2)) STRICT")
        .unwrap();
    conn.execute("CREATE TABLE w(k int PRIMARY KEY, a integer[], n numeric(10,2)) STRICT")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER w_grew AFTER UPDATE ON w WHEN NEW.a > OLD.a AND NEW.n > OLD.n \
         BEGIN SELECT 1; END",
    )
    .unwrap();
    conn
}
