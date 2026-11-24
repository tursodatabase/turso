//! Known tursopg gaps against PostgreSQL that `.sqltest` files cannot
//! express: bound parameters, a database reopen, two commands in one prepared
//! statement, MVCC, and a query after a failed statement.
//!
//! A test of a gap that matches PostgreSQL asserts the PostgreSQL result. A
//! test of an open gap has `tursopg_returns_wrong_result` in its name and
//! asserts the current wrong tursopg result. A comment above it gives the
//! PostgreSQL result. When you fix a gap, its test fails: change the assert to
//! the PostgreSQL result and remove `tursopg_returns_wrong_result` from the
//! name. See postgres/CONTRIBUTING.md.
//!
//! The gap counts of `make -C postgres/conformance gaps` come from the `@compat-gap`
//! tests in the .sqltest files and do not include these tests.
//!
//! Results are rows joined by ` ; ` and cells joined by `|`, or
//! `ERROR <sqlstate>`. PostgreSQL results are the PostgreSQL 18 results in
//! the form the embedded API returns them: booleans as `1` and `0`, and
//! timestamptz in UTC without the offset.

use crate::common::TempDatabase;
use std::num::NonZero;
use turso_core::{Numeric, Value};

// A-03 (S1): bool $n param cast (text param)
// tursopg: boolean text params all cast to false.
// PostgreSQL: 1|1|0|1
#[turso_macros::test(views)]
fn gap_a_03_tursopg_returns_wrong_result_bool_n_param_cast_text_param(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT $1::boolean, $2::boolean, $3::boolean, $4::boolean",
        params: &[
            P::Text("true"),
            P::Text("t"),
            P::Text("false"),
            P::Text("on"),
        ],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "0|0|0|0");
}

// A-05 (S1): bool invalid param
// tursopg: an invalid boolean param casts to false instead of failing.
// PostgreSQL: ERROR 22P02
#[turso_macros::test(views)]
fn gap_a_05_tursopg_returns_wrong_result_bool_invalid_param(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT $1::boolean",
        params: &[P::Text("maybe")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "0");
}

// A-12 (S1): invalid int param cast
// tursopg: an invalid bigint param casts to 0 instead of failing.
// PostgreSQL: ERROR 22P02
#[turso_macros::test(views)]
fn gap_a_12_tursopg_returns_wrong_result_invalid_int_param_cast(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT $1::bigint",
        params: &[P::Text("abc")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "0");
}

// A-15: int param text -> int arithmetic
#[turso_macros::test(views)]
fn gap_a_15_int_param_text_int_arithmetic(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT $1::int + $2::int",
        params: &[P::Text("1"), P::Text("2")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "3");
}

// A-18 (S1): invalid uuid param cast
// tursopg: an invalid uuid param is accepted.
// PostgreSQL: ERROR 22P02
#[turso_macros::test(views)]
fn gap_a_18_tursopg_returns_wrong_result_invalid_uuid_param_cast(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT $1::uuid",
        params: &[P::Text("nope")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "nope");
}

// A-29 (S2): numeric $n param
// tursopg: a numeric(p,s) cast of a param is not supported.
// PostgreSQL: 12.35
#[turso_macros::test(views)]
fn gap_a_29_tursopg_returns_wrong_result_numeric_n_param(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT $1::numeric(10,2)",
        params: &[P::Text("12.345")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "ERROR 0A000");
}

// A-33: timestamptz $n param with offset
#[turso_macros::test(views)]
fn gap_a_33_timestamptz_n_param_with_offset(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT $1::timestamptz",
        params: &[P::Text("2026-10-08T14:00:00+02:00")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "2026-10-08 12:00:00");
}

// A-41: date $n param
#[turso_macros::test(views)]
fn gap_a_41_date_n_param(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT $1::date",
        params: &[P::Text("2026-10-08")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "2026-10-08");
}

// A-49 (S1): invalid jsonb param
// tursopg: an invalid jsonb param is accepted.
// PostgreSQL: ERROR 22P02
#[turso_macros::test(views)]
fn gap_a_49_tursopg_returns_wrong_result_invalid_jsonb_param(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT $1::jsonb",
        params: &[P::Text("{bad")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "{bad");
}

// A-53: = ANY($1::bigint[]) text param
#[turso_macros::test(views)]
fn gap_a_53_any_1_bigint_text_param(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE v (x bigint)",
            "INSERT INTO v VALUES (1), (2), (3)",
        ],
        sql: "SELECT x FROM v WHERE x = ANY($1::bigint[]) ORDER BY x",
        params: &[P::Text("{1,3}")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1 ; 3");
}

// A-54: = ANY($1::text[]) text param
#[turso_macros::test(views)]
fn gap_a_54_any_1_text_text_param(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE v (x text)",
            "INSERT INTO v VALUES ('a'), ('b'), ('c')",
        ],
        sql: "SELECT x FROM v WHERE x = ANY($1::text[]) ORDER BY x",
        params: &[P::Text("{a,c}")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "a ; c");
}

// A-55: <> ALL($1::int[])
#[turso_macros::test(views)]
fn gap_a_55_all_1_int(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE v (x int)",
            "INSERT INTO v VALUES (1), (2), (3)",
        ],
        sql: "SELECT x FROM v WHERE x <> ALL($1::int[]) ORDER BY x",
        params: &[P::Text("{1,3}")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "2");
}

// A-56: = ANY($1::uuid[])
#[turso_macros::test(views)]
fn gap_a_56_any_1_uuid(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT $2::uuid = ANY($1::uuid[])",
        params: &[
            P::Text("{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"),
            P::Text("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
        ],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1");
}

// A-57: text[] param with quotes/commas
#[turso_macros::test(views)]
fn gap_a_57_text_param_with_quotes_commas(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT ($1::text[])[2]",
        params: &[P::Text(r#"{a,"b,c",d}"#)],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "b,c");
}

// A-67: untyped $1 text '42' into int column
#[turso_macros::test(views)]
fn gap_a_67_untyped_1_text_42_into_int_column(db: TempDatabase) {
    let out = GapCase {
        setup: &["CREATE TABLE t (id int)"],
        sql: "INSERT INTO t VALUES ($1) RETURNING id",
        params: &[P::Text("42")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "42");
}

// A-68: untyped $1 text 'true' into bool column
#[turso_macros::test(views)]
fn gap_a_68_untyped_1_text_true_into_bool_column(db: TempDatabase) {
    let out = GapCase {
        setup: &["CREATE TABLE t (b boolean)"],
        sql: "INSERT INTO t VALUES ($1) RETURNING b",
        params: &[P::Text("true")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1");
}

// A-69: untyped $1 '2026-10-08 14:00+02' into timestamptz column
#[turso_macros::test(views)]
fn gap_a_69_untyped_1_2026_10_08_14_00_02_into_timestamptz_c(db: TempDatabase) {
    let out = GapCase {
        setup: &["CREATE TABLE t (ts timestamptz)"],
        sql: "INSERT INTO t VALUES ($1) RETURNING ts",
        params: &[P::Text("2026-10-08 14:00:00+02")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "2026-10-08 12:00:00");
}

// A-70 (S1): insert jsonb text param into jsonb column
// tursopg: a jsonb param is not normalized.
// PostgreSQL: {"a": 2, "b": 1}
#[turso_macros::test(views)]
fn gap_a_70_tursopg_returns_wrong_result_insert_jsonb_text_param_into_jsonb_column(
    db: TempDatabase,
) {
    let out = GapCase {
        setup: &["CREATE TABLE t (j jsonb)"],
        sql: "INSERT INTO t VALUES ($1) RETURNING j",
        params: &[P::Text(r#"{"b": 1, "a": 2}"#)],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, r#"{"b":1,"a":2}"#);
}

// A-71: where bool col = $1 text 'true'
#[turso_macros::test(views)]
fn gap_a_71_where_bool_col_1_text_true(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int, b boolean)",
            "INSERT INTO t VALUES (1, true), (2, false)",
        ],
        sql: "SELECT id FROM t WHERE b = $1",
        params: &[P::Text("true")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1");
}

// A-72: where bool col = $1::boolean text 'false'
#[turso_macros::test(views)]
fn gap_a_72_where_bool_col_1_boolean_text_false(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int, b boolean)",
            "INSERT INTO t VALUES (1, true), (2, false)",
        ],
        sql: "SELECT id FROM t WHERE b = $1::boolean",
        params: &[P::Text("false")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "2");
}

// A-73: where int col = $1 text param
#[turso_macros::test(views)]
fn gap_a_73_where_int_col_1_text_param(db: TempDatabase) {
    let out = GapCase {
        setup: &["CREATE TABLE t (id int)", "INSERT INTO t VALUES (1), (2)"],
        sql: "SELECT id FROM t WHERE id = $1",
        params: &[P::Text("2")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "2");
}

// A-74: where uuid col = $1 uppercase
#[turso_macros::test(views)]
fn gap_a_74_where_uuid_col_1_uppercase(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (u uuid)",
            "INSERT INTO t VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')",
        ],
        sql: "SELECT u FROM t WHERE u = $1",
        params: &[P::Text("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
}

// A-75: where tstz col > $1 with offset
#[turso_macros::test(views)]
fn gap_a_75_where_tstz_col_1_with_offset(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (ts timestamptz)",
            "INSERT INTO t VALUES ('2026-10-08T12:00:00Z')",
        ],
        sql: "SELECT count(*) FROM t WHERE ts > $1::timestamptz",
        params: &[P::Text("2026-10-08 13:00:00+02")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1");
}

// A-76: untyped $1 param result type
#[turso_macros::test(views)]
fn gap_a_76_untyped_1_param_result_type(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT $1",
        params: &[P::Text("x")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "x");
}

// A-77: insert int param bound as int into text column
#[turso_macros::test(views)]
fn gap_a_77_insert_int_param_bound_as_int_into_text_column(db: TempDatabase) {
    let out = GapCase {
        setup: &["CREATE TABLE t (s text)"],
        sql: "INSERT INTO t VALUES ($1) RETURNING s",
        params: &[P::Int(5)],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "5");
}

// A-78: NULL param
#[turso_macros::test(views)]
fn gap_a_78_null_param(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT $1::int IS NULL",
        params: &[P::Null],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1");
}

// B-13: LIKE with $1 param
#[turso_macros::test(views)]
fn gap_b_13_like_with_1_param(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int PRIMARY KEY, s text, n int, arr text[], ia int[], j jsonb, ok boolean)",
            r#"INSERT INTO t VALUES (1, 'abc', 10, '{a,b}', '{1,2}', '{"a":{"b":1},"n":1,"tags":["x","y"]}', true), (2, 'Abd', NULL, '{c}', '{3}', '{"a":2,"n":2}', false), (3, 'a_c', 30, '{}', '{}', NULL, NULL), (4, 'a%c', 5, NULL, NULL, '[1,2]', NULL)"#,
        ],
        sql: "SELECT id FROM t WHERE s LIKE $1 ORDER BY id",
        params: &[P::Text("a%")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1 ; 3 ; 4");
}

// B-24: IN list with params
#[turso_macros::test(views)]
fn gap_b_24_in_list_with_params(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int PRIMARY KEY, s text, n int, arr text[], ia int[], j jsonb, ok boolean)",
            r#"INSERT INTO t VALUES (1, 'abc', 10, '{a,b}', '{1,2}', '{"a":{"b":1},"n":1,"tags":["x","y"]}', true), (2, 'Abd', NULL, '{c}', '{3}', '{"a":2,"n":2}', false), (3, 'a_c', 30, '{}', '{}', NULL, NULL), (4, 'a%c', 5, NULL, NULL, '[1,2]', NULL)"#,
        ],
        sql: "SELECT id FROM t WHERE id IN ($1, $2) ORDER BY id",
        params: &[P::Text("1"), P::Text("3")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1 ; 3");
}

// B-27 (S2): col > ANY($1::int[])
// tursopg: > ANY with an array param fails with a syntax error.
// PostgreSQL: 3
#[turso_macros::test(views)]
fn gap_b_27_tursopg_returns_wrong_result_col_any_1_int(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int PRIMARY KEY, s text, n int, arr text[], ia int[], j jsonb, ok boolean)",
            r#"INSERT INTO t VALUES (1, 'abc', 10, '{a,b}', '{1,2}', '{"a":{"b":1},"n":1,"tags":["x","y"]}', true), (2, 'Abd', NULL, '{c}', '{3}', '{"a":2,"n":2}', false), (3, 'a_c', 30, '{}', '{}', NULL, NULL), (4, 'a%c', 5, NULL, NULL, '[1,2]', NULL)"#,
        ],
        sql: "SELECT id FROM t WHERE n > ANY($1::int[]) ORDER BY id",
        params: &[P::Text("{20,40}")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "ERROR 42601");
}

// B-32: int[] @> $1::int[]
#[turso_macros::test(views)]
fn gap_b_32_int_1_int(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int PRIMARY KEY, s text, n int, arr text[], ia int[], j jsonb, ok boolean)",
            r#"INSERT INTO t VALUES (1, 'abc', 10, '{a,b}', '{1,2}', '{"a":{"b":1},"n":1,"tags":["x","y"]}', true), (2, 'Abd', NULL, '{c}', '{3}', '{"a":2,"n":2}', false), (3, 'a_c', 30, '{}', '{}', NULL, NULL), (4, 'a%c', 5, NULL, NULL, '[1,2]', NULL)"#,
        ],
        sql: "SELECT id FROM t WHERE ia @> $1::int[] ORDER BY id",
        params: &[P::Text("{1}")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1");
}

// B-44: jsonb @> $1::jsonb param
#[turso_macros::test(views)]
fn gap_b_44_jsonb_1_jsonb_param(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int PRIMARY KEY, s text, n int, arr text[], ia int[], j jsonb, ok boolean)",
            r#"INSERT INTO t VALUES (1, 'abc', 10, '{a,b}', '{1,2}', '{"a":{"b":1},"n":1,"tags":["x","y"]}', true), (2, 'Abd', NULL, '{c}', '{3}', '{"a":2,"n":2}', false), (3, 'a_c', 30, '{}', '{}', NULL, NULL), (4, 'a%c', 5, NULL, NULL, '[1,2]', NULL)"#,
        ],
        sql: "SELECT id FROM t WHERE j @> $1::jsonb ORDER BY id",
        params: &[P::Text(r#"{"a":2}"#)],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "2");
}

// B-54: jsonb ->  param key
#[turso_macros::test(views)]
fn gap_b_54_jsonb_param_key(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int PRIMARY KEY, s text, n int, arr text[], ia int[], j jsonb, ok boolean)",
            r#"INSERT INTO t VALUES (1, 'abc', 10, '{a,b}', '{1,2}', '{"a":{"b":1},"n":1,"tags":["x","y"]}', true), (2, 'Abd', NULL, '{c}', '{3}', '{"a":2,"n":2}', false), (3, 'a_c', 30, '{}', '{}', NULL, NULL), (4, 'a%c', 5, NULL, NULL, '[1,2]', NULL)"#,
        ],
        sql: "SELECT j -> $1::text ->> $2::text FROM t WHERE id = 1",
        params: &[P::Text("a"), P::Text("b")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1");
}

// B-89: LIMIT/OFFSET with text params
#[turso_macros::test(views)]
fn gap_b_89_limit_offset_with_text_params(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int PRIMARY KEY, s text, n int, arr text[], ia int[], j jsonb, ok boolean)",
            r#"INSERT INTO t VALUES (1, 'abc', 10, '{a,b}', '{1,2}', '{"a":{"b":1},"n":1,"tags":["x","y"]}', true), (2, 'Abd', NULL, '{c}', '{3}', '{"a":2,"n":2}', false), (3, 'a_c', 30, '{}', '{}', NULL, NULL), (4, 'a%c', 5, NULL, NULL, '[1,2]', NULL)"#,
        ],
        sql: "SELECT id FROM t ORDER BY id LIMIT $1 OFFSET $2",
        params: &[P::Text("2"), P::Text("1")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "2 ; 3");
}

// B-118 (S2): json_populate_recordset
// tursopg: json_populate_recordset in FROM is refused with 0A000.
// PostgreSQL: 1|a
#[turso_macros::test(views)]
fn gap_b_118_tursopg_returns_wrong_result_json_populate_recordset(db: TempDatabase) {
    let out = GapCase {
        setup: &["CREATE TABLE r (id int, s text)"],
        sql: "SELECT id, s FROM json_populate_recordset(null::r, $1)",
        params: &[P::Text(r#"[{"id":1,"s":"a"}]"#)],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "ERROR 0A000");
}

// B-119: json_to_recordset with column defs
#[turso_macros::test(views)]
fn gap_b_119_json_to_recordset_with_column_defs(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT id, s FROM json_to_recordset($1) AS x(id int, s text)",
        params: &[P::Text(r#"[{"id":1,"s":"a"}]"#)],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1|a");
}

// B-125: bound LIMIT in a correlated subquery
#[turso_macros::test(views)]
fn gap_b_125_bound_limit_in_a_correlated_subquery(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE p (id int PRIMARY KEY)",
            "CREATE TABLE c (id int PRIMARY KEY, pid int)",
            "INSERT INTO p VALUES (1), (2), (3)",
            "INSERT INTO c VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 3), (6, 3)",
        ],
        sql: "SELECT p.id, (SELECT count(*) FROM (SELECT c.id FROM c WHERE c.pid = p.id ORDER BY c.id LIMIT $1) s) FROM p ORDER BY p.id",
        params: &[P::Text("1")],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1|1 ; 2|1 ; 3|1");
}

// D-43: statement after error in tx (25P02)
#[turso_macros::test(views)]
fn gap_d_43_statement_after_error_in_tx_25p02(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int PRIMARY KEY)",
            "BEGIN",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO t VALUES (1)",
        ],
        sql: "SELECT count(*) FROM t",
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "[SETUP-ERR 23505] ERROR 25P02");
}

// D-50 (S1): prepared statement with two commands
// tursopg: a prepared statement with two commands runs the first one.
// PostgreSQL: ERROR 42601
#[turso_macros::test(views)]
fn gap_d_50_tursopg_returns_wrong_result_prepared_statement_with_two_commands(db: TempDatabase) {
    let out = GapCase {
        sql: "SELECT 1; SELECT 2",
        fk_off: true,
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1");
}

// E-36: schema survives reopen
#[turso_macros::test(views)]
fn gap_e_36_schema_survives_reopen(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE SCHEMA api",
            "CREATE TABLE api.items (id int PRIMARY KEY)",
            "INSERT INTO api.items VALUES (1)",
        ],
        sql: "SELECT id FROM api.items",
        reopen: true,
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1");
}

// E-71: ALTER TABLE RENAME TO survives reopen
#[turso_macros::test(views)]
fn gap_e_71_alter_table_rename_to_survives_reopen(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int PRIMARY KEY, s text)",
            "INSERT INTO t VALUES (1, 'a')",
            "ALTER TABLE t RENAME TO t2",
        ],
        sql: "SELECT id FROM t2",
        reopen: true,
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "1");
}

// E-72: ALTER TABLE RENAME COLUMN survives reopen
#[turso_macros::test(views)]
fn gap_e_72_alter_table_rename_column_survives_reopen(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int PRIMARY KEY, s text)",
            "INSERT INTO t VALUES (1, 'a')",
            "ALTER TABLE t RENAME COLUMN s TO s2",
        ],
        sql: "SELECT s2 FROM t",
        reopen: true,
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "a");
}

// E-75: in-memory schema stays in memory
#[turso_macros::test(views)]
fn gap_e_75_in_memory_schema_stays_in_memory(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE SCHEMA api",
            "CREATE TABLE api.items (id int PRIMARY KEY)",
            "INSERT INTO api.items VALUES (1)",
        ],
        sql: "SELECT id FROM api.items",
        reopen: true,
        reopen_setup: &["CREATE SCHEMA api"],
        memory: true,
        fk_off: true,
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "ERROR 42P01");
}

// E-76: ADD COLUMN on a table with a function default survives reopen
#[turso_macros::test(views)]
fn gap_e_76_add_column_on_a_table_with_a_function_default_su(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), s text)",
            "ALTER TABLE t ADD COLUMN done boolean NOT NULL DEFAULT false",
        ],
        sql: "SELECT count(done) FROM t",
        reopen: true,
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "0");
}

// E-77: DDL in a schema right after CREATE SCHEMA (MVCC)
#[turso_macros::test(views)]
fn gap_e_77_ddl_in_a_schema_right_after_create_schema_mvcc(db: TempDatabase) {
    let out = GapCase {
        setup: &["CREATE SCHEMA IF NOT EXISTS auth"],
        sql: "CREATE TABLE auth.users (id uuid PRIMARY KEY)",
        mvcc: true,
        fk_off: true,
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "");
}

// E-78 (S2): CREATE MATERIALIZED VIEW then select (MVCC)
// tursopg: CREATE MATERIALIZED VIEW fails with a syntax error (42601) in MVCC mode.
// PostgreSQL: 1
#[turso_macros::test(views)]
fn gap_e_78_tursopg_returns_wrong_result_create_materialized_view_then_select_mvcc(
    db: TempDatabase,
) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int)",
            "INSERT INTO t VALUES (1)",
            "CREATE MATERIALIZED VIEW mv AS SELECT id FROM t",
        ],
        sql: "SELECT id FROM mv",
        mvcc: true,
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "[SETUP-ERR 42601] ERROR 42P01");
}

// E-83: enum column rejects an unknown label after reopen
#[turso_macros::test(views)]
fn gap_e_83_enum_column_rejects_an_unknown_label_after_reope(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TYPE mood AS ENUM ('sad', 'happy')",
            "CREATE TABLE t (id int, m mood)",
        ],
        sql: "INSERT INTO t VALUES (1, 'angry')",
        reopen: true,
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "ERROR 22P02");
}

// E-84: enum type survives reopen
#[turso_macros::test(views)]
fn gap_e_84_enum_type_survives_reopen(db: TempDatabase) {
    let out = GapCase {
        setup: &["CREATE TYPE mood AS ENUM ('sad', 'happy')"],
        sql: "CREATE TABLE t (id int, m mood)",
        reopen: true,
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "");
}

// E-88: DEFAULT auth.uid() / auth.email() fill the claims after reopen
// PostgreSQL first creates the auth functions that tursopg has built in.
#[turso_macros::test(views)]
fn gap_e_88_default_auth_uid_auth_email_fill_the_claims_afte(db: TempDatabase) {
    let out = GapCase {
        setup: &[
            "CREATE TABLE t (id int, owner uuid DEFAULT auth.uid(), email text DEFAULT auth.email())",
        ],
        sql: "INSERT INTO t (id) VALUES (1) RETURNING owner, email",
        reopen: true,
        reopen_setup: &[
            r#"SELECT set_config('request.jwt.claims', '{"sub":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","email":"a@b.co","role":"authenticated"}', false)"#,
        ],
        ..GapCase::default()
    }
    .run(db);
    assert_eq!(out, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|a@b.co");
}

/// A bound parameter: text, an integer or NULL.
enum P {
    Text(&'static str),
    Int(i64),
    Null,
}

#[derive(Default)]
struct GapCase {
    setup: &'static [&'static str],
    sql: &'static str,
    params: &'static [P],
    /// Close and reopen the database between setup and query.
    reopen: bool,
    /// Statements on the reopened database before the query.
    reopen_setup: &'static [&'static str],
    /// A `:memory:` database: reopening gives a new, empty database.
    memory: bool,
    /// `PRAGMA journal_mode = 'mvcc'` right after open.
    mvcc: bool,
    /// Leave foreign keys off.
    fk_off: bool,
}

impl GapCase {
    fn run(&self, db: TempDatabase) -> String {
        let mut out = String::new();
        let mut conn = self.connect(&db);
        for sql in self.setup {
            if let Err(e) = conn.execute(sql) {
                out.push_str(&format!("[SETUP-ERR {}] ", e.sqlstate));
            }
        }
        let mut db = db;
        if self.memory || self.reopen {
            drop(conn);
            db = if self.memory {
                TempDatabase::builder().build()
            } else {
                db.reopen()
            };
            conn = self.connect(&db);
        }
        for sql in self.reopen_setup {
            if let Err(e) = conn.execute(sql) {
                out.push_str(&format!("[SETUP-ERR {}] ", e.sqlstate));
            }
        }
        match self.query(&conn) {
            Ok(rows) => out.push_str(&rows),
            Err(e) => out.push_str(&format!("ERROR {}", e.sqlstate)),
        }
        out
    }

    fn connect(&self, db: &TempDatabase) -> turso_pg::Connection {
        let conn = db.connect_postgres();
        if self.mvcc {
            conn.inner()
                .pragma_update("journal_mode", "'mvcc'")
                .unwrap();
        }
        if !self.fk_off {
            conn.inner().set_foreign_keys_enabled(true);
        }
        conn
    }

    fn query(&self, conn: &turso_pg::Connection) -> turso_pg::Result<String> {
        let mut stmt = conn.prepare(self.sql)?;
        for (i, p) in self.params.iter().enumerate() {
            let value = match p {
                P::Text(s) => Value::build_text(*s),
                P::Int(n) => Value::from_i64(*n),
                P::Null => Value::Null,
            };
            stmt.bind_at(NonZero::new(i + 1).unwrap(), value)?;
        }
        let rows = stmt.run_collect_rows()?;
        Ok(rows
            .iter()
            .map(|row| row.iter().map(cell).collect::<Vec<_>>().join("|"))
            .collect::<Vec<_>>()
            .join(" ; "))
    }
}

fn cell(value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Numeric(Numeric::Integer(i)) => i.to_string(),
        Value::Numeric(Numeric::Float(f)) => f64::from(*f).to_string(),
        Value::Text(t) => t.as_str().to_owned(),
        Value::Blob(b) => b
            .iter()
            .fold("\\x".to_owned(), |s, x| s + &format!("{x:02x}")),
    }
}
