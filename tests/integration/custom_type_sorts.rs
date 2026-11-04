//! Every sort of a custom type column puts the values in the order of `<`.
//!
//! For every kind of type and every statement that sorts, the test reads the
//! sorted values and asks the engine whether `b < a` for each pair of
//! neighbours `a, b` (or `a < b` for a descending sort). The answer must be no.

#[cfg(test)]
mod tests {
    use crate::common::{limbo_exec_rows, TempDatabase};
    use rusqlite::types::Value;
    use std::sync::Arc;

    struct TypeCase {
        name: &'static str,
        create: &'static [&'static str],
        column_type: &'static str,
        /// SQL literals. Each table also gets a NULL.
        values: &'static [&'static str],
    }

    const TYPES: &[TypeCase] = &[
        TypeCase {
            name: "built-in numeric",
            create: &[],
            column_type: "numeric(10,2)",
            values: &["10", "9", "-1.5", "100", "9.00", "0.5"],
        },
        TypeCase {
            name: "built-in date",
            create: &[],
            column_type: "date",
            values: &["'2024-03-15'", "'2023-12-31'", "'2024-01-01'", "'2024-01-01'"],
        },
        TypeCase {
            name: "DECODE value",
            create: &["CREATE TYPE up BASE text ENCODE upper(value) DECODE value OPERATOR '<'"],
            column_type: "up",
            values: &["'b'", "'A'", "'c'", "'a'", "'B'"],
        },
        TypeCase {
            name: "DECODE reverses the order",
            create: &[
                "CREATE TYPE neg(value integer) BASE integer ENCODE -value DECODE -value OPERATOR '<'",
            ],
            column_type: "neg",
            values: &["3", "1", "10", "2", "1", "-4"],
        },
        TypeCase {
            name: "DECODE reverses the text",
            create: &["CREATE TYPE rev BASE text ENCODE string_reverse(value) DECODE string_reverse(value) OPERATOR '<'"],
            column_type: "rev",
            values: &["'apple'", "'banana'", "'cherry'", "'ab'", "'ba'"],
        },
        TypeCase {
            name: "DECODE merges values",
            create: &["CREATE TYPE ci BASE text ENCODE value DECODE lower(value) OPERATOR '<'"],
            column_type: "ci",
            values: &["'B'", "'a'", "'A'", "'c'", "'b'"],
        },
        TypeCase {
            name: "DECODE keeps the order",
            create: &[
                "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100 OPERATOR '<'",
            ],
            column_type: "cents",
            values: &["10", "9", "2", "-3", "9"],
        },
        TypeCase {
            name: "domain over a type whose DECODE reverses the order",
            create: &[
                "CREATE TYPE neg(value integer) BASE integer ENCODE -value DECODE -value OPERATOR '<'",
                "CREATE DOMAIN dneg AS neg",
            ],
            column_type: "dneg",
            values: &["3", "1", "10", "2", "1"],
        },
        TypeCase {
            name: "domain over integer",
            create: &["CREATE DOMAIN posint AS integer CHECK (value > 0)"],
            column_type: "posint",
            values: &["5", "3", "9", "3", "12"],
        },
        TypeCase {
            name: "integer array",
            create: &[],
            column_type: "integer[]",
            values: &["'{2}'", "'{1,2}'", "'{1}'", "'{10}'", "'{1,2}'"],
        },
        TypeCase {
            name: "text array",
            create: &[],
            column_type: "text[]",
            values: &["'{b}'", "'{a,b}'", "'{a}'", "'{B}'", "'{a}'"],
        },
    ];

    #[derive(Clone, Copy, Debug)]
    enum Direction {
        Asc,
        Desc,
    }

    struct SortCase {
        name: &'static str,
        /// Returns `(group, c)` rows. Neighbours are only compared within a
        /// group.
        sql: &'static str,
        direction: Direction,
        nulls_first: bool,
    }

    const SORTS: &[SortCase] = &[
        SortCase {
            name: "ORDER BY",
            sql: "SELECT 0, c FROM t ORDER BY c",
            direction: Direction::Asc,
            nulls_first: true,
        },
        SortCase {
            name: "ORDER BY with LIMIT",
            sql: "SELECT 0, c FROM t ORDER BY c LIMIT 100",
            direction: Direction::Asc,
            nulls_first: true,
        },
        SortCase {
            name: "ORDER BY DESC",
            sql: "SELECT 0, c FROM t ORDER BY c DESC",
            direction: Direction::Desc,
            nulls_first: false,
        },
        SortCase {
            name: "ORDER BY DESC with LIMIT",
            sql: "SELECT 0, c FROM t ORDER BY c DESC LIMIT 100",
            direction: Direction::Desc,
            nulls_first: false,
        },
        SortCase {
            name: "ORDER BY NULLS LAST",
            sql: "SELECT 0, c FROM t ORDER BY c NULLS LAST",
            direction: Direction::Asc,
            nulls_first: false,
        },
        SortCase {
            name: "ORDER BY DESC NULLS FIRST",
            sql: "SELECT 0, c FROM t ORDER BY c DESC NULLS FIRST",
            direction: Direction::Desc,
            nulls_first: true,
        },
        SortCase {
            name: "ORDER BY a column that is not a result column",
            sql: "SELECT 0, (SELECT c FROM t y WHERE y.id = t.id) FROM t ORDER BY c",
            direction: Direction::Asc,
            nulls_first: true,
        },
        SortCase {
            name: "ORDER BY two columns",
            sql: "SELECT g, c FROM t ORDER BY g, c",
            direction: Direction::Asc,
            nulls_first: true,
        },
        SortCase {
            name: "ORDER BY two columns DESC",
            sql: "SELECT g, c FROM t ORDER BY g DESC, c DESC",
            direction: Direction::Desc,
            nulls_first: false,
        },
        SortCase {
            name: "window ORDER BY",
            sql: "SELECT 0, c FROM (SELECT c, row_number() OVER (ORDER BY c) AS r FROM t) ORDER BY r",
            direction: Direction::Asc,
            nulls_first: true,
        },
        SortCase {
            name: "window ORDER BY DESC",
            sql: "SELECT 0, c FROM (SELECT c, row_number() OVER (ORDER BY c DESC) AS r FROM t) ORDER BY r",
            direction: Direction::Desc,
            nulls_first: false,
        },
        SortCase {
            name: "window PARTITION BY",
            sql: "SELECT 0, c FROM (SELECT c, count(*) OVER (PARTITION BY c) FROM t)",
            direction: Direction::Asc,
            nulls_first: true,
        },
        SortCase {
            name: "window PARTITION BY and ORDER BY",
            sql: "SELECT g, c FROM (SELECT g, c, row_number() OVER (PARTITION BY g ORDER BY c) FROM t)",
            direction: Direction::Asc,
            nulls_first: true,
        },
        SortCase {
            name: "GROUP BY",
            sql: "SELECT 0, c FROM t GROUP BY c",
            direction: Direction::Asc,
            nulls_first: true,
        },
        SortCase {
            name: "GROUP BY with ORDER BY",
            sql: "SELECT 0, c FROM t GROUP BY c ORDER BY c DESC",
            direction: Direction::Desc,
            nulls_first: false,
        },
        SortCase {
            name: "DISTINCT with ORDER BY",
            sql: "SELECT DISTINCT 0, c FROM t ORDER BY c",
            direction: Direction::Asc,
            nulls_first: true,
        },
        SortCase {
            name: "UNION with ORDER BY",
            sql: "SELECT 0, c FROM t UNION SELECT 0, c FROM t ORDER BY 2",
            direction: Direction::Asc,
            nulls_first: true,
        },
        SortCase {
            name: "UNION ALL with ORDER BY DESC",
            sql: "SELECT 0, c FROM t UNION ALL SELECT 0, c FROM t ORDER BY 2 DESC",
            direction: Direction::Desc,
            nulls_first: false,
        },
    ];

    #[test]
    fn test_every_sort_of_a_custom_type_agrees_with_less_than() {
        let mut failures = Vec::new();
        for mvcc in [false, true] {
            for type_case in TYPES {
                let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
                let db = TempDatabase::builder()
                    .with_opts(opts)
                    .with_mvcc(mvcc)
                    .build();
                let conn = db.connect_limbo();
                for sql in type_case.create {
                    conn.execute(*sql).unwrap();
                }
                conn.execute(format!(
                    "CREATE TABLE t(id INTEGER PRIMARY KEY, c {}, g integer) STRICT",
                    type_case.column_type
                ))
                .unwrap();
                for (i, value) in type_case.values.iter().chain(&["NULL"]).enumerate() {
                    conn.execute(format!("INSERT INTO t VALUES ({i}, {value}, {})", i % 2))
                        .unwrap();
                }
                let label = format!("{} (mvcc={mvcc})", type_case.name);
                for sort in SORTS {
                    let rows = limbo_exec_rows(&conn, sort.sql);
                    check_sorted(&conn, &rows, sort, &label, &mut failures);
                }
                check_min_max(&conn, &label, &mut failures);
                conn.close().unwrap();
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    /// Checks that NULLs come first or last and that no neighbour is out of
    /// the order of `<`.
    fn check_sorted(
        conn: &Arc<turso_core::Connection>,
        rows: &[Vec<Value>],
        sort: &SortCase,
        label: &str,
        failures: &mut Vec<String>,
    ) {
        let values: Vec<&Value> = rows.iter().map(|row| &row[1]).collect();
        for pair in rows.windows(2) {
            let (a, b) = (&pair[0], &pair[1]);
            if a[0] != b[0] {
                continue;
            }
            let (a, b) = (&a[1], &b[1]);
            match (a, b) {
                (Value::Null, Value::Null) => {}
                (Value::Null, _) | (_, Value::Null) => {
                    let null_came_first = *a == Value::Null;
                    if null_came_first != sort.nulls_first {
                        failures.push(format!(
                            "{label}, {}: NULL is in the wrong place in {values:?}",
                            sort.name
                        ));
                    }
                }
                _ => {
                    let (first, second) = match sort.direction {
                        Direction::Asc => (b, a),
                        Direction::Desc => (a, b),
                    };
                    if is_less(conn, first, second) {
                        failures.push(format!(
                            "{label}, {}: {first:?} < {second:?}, but they are in the order {values:?}",
                            sort.name
                        ));
                    }
                }
            }
        }
    }

    /// Checks that no value is less than min() and max() is less than none.
    fn check_min_max(conn: &Arc<turso_core::Connection>, label: &str, failures: &mut Vec<String>) {
        let min_max = limbo_exec_rows(conn, "SELECT min(c), max(c) FROM t");
        let (min, max) = (&min_max[0][0], &min_max[0][1]);
        for row in limbo_exec_rows(conn, "SELECT c FROM t WHERE c IS NOT NULL") {
            let value = &row[0];
            if is_less(conn, value, min) {
                failures.push(format!("{label}: {value:?} < min() {min:?}"));
            }
            if is_less(conn, max, value) {
                failures.push(format!("{label}: max() {max:?} < {value:?}"));
            }
        }
    }

    /// `a < b` as the engine compares two values of column `c`. The values are
    /// what the column reads back, so they are found by `=` on the column.
    fn is_less(conn: &Arc<turso_core::Connection>, a: &Value, b: &Value) -> bool {
        let id_a = id_of(conn, a);
        let id_b = id_of(conn, b);
        let rows = limbo_exec_rows(
            conn,
            &format!("SELECT x.c < y.c FROM t x, t y WHERE x.id = {id_a} AND y.id = {id_b}"),
        );
        rows == vec![vec![Value::Integer(1)]]
    }

    fn id_of(conn: &Arc<turso_core::Connection>, value: &Value) -> i64 {
        limbo_exec_rows(conn, "SELECT id, c FROM t")
            .into_iter()
            .find(|row| &row[1] == value)
            .map(|row| match row[0] {
                Value::Integer(id) => id,
                ref other => panic!("id is not an integer: {other:?}"),
            })
            .unwrap_or_else(|| panic!("no row reads back {value:?}"))
    }
}
