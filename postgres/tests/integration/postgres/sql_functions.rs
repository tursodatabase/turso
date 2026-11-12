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

    // PostgreSQL looks for the function before it checks the owner, so a
    // role that owns nothing gets the results for a missing function.
    pg.execute("DROP FUNCTION IF EXISTS nosuch()").unwrap();
    let missing = pg.execute("DROP FUNCTION nosuch()").unwrap_err();
    assert_eq!(missing.sqlstate, "42883");
}

fn integers(statement: &mut turso_pg::Statement) -> Vec<i64> {
    let rows = statement.run_collect_rows().unwrap();
    statement.reset().unwrap();
    rows.iter().map(|row| row[0].as_int().unwrap()).collect()
}

#[test]
fn a_prepared_call_uses_the_function_as_it_is_when_the_statement_runs() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    pg.execute("CREATE FUNCTION amount() RETURNS integer LANGUAGE sql AS $$SELECT 1$$")
        .unwrap();
    let mut call = pg.prepare("SELECT amount()").unwrap();
    assert_eq!(integers(&mut call), [1]);

    pg.execute("CREATE OR REPLACE FUNCTION amount() RETURNS integer LANGUAGE sql AS $$SELECT 2$$")
        .unwrap();
    assert_eq!(integers(&mut call), [2]);

    pg.execute("DROP FUNCTION amount()").unwrap();
    let error = call.run_collect_rows().unwrap_err();
    assert_eq!(error.to_string(), "no such function: amount");
}

#[test]
fn a_prepared_call_uses_the_search_path_of_the_time_it_runs() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    pg.execute("CREATE SCHEMA one").unwrap();
    pg.execute("CREATE SCHEMA two").unwrap();
    pg.execute("CREATE FUNCTION one.amount() RETURNS integer LANGUAGE sql AS $$SELECT 1$$")
        .unwrap();
    pg.execute("CREATE FUNCTION two.amount() RETURNS integer LANGUAGE sql AS $$SELECT 2$$")
        .unwrap();
    pg.execute(
        "CREATE FUNCTION public.next_amount() RETURNS integer LANGUAGE sql \
         AS $$SELECT one.amount() + 1$$",
    )
    .unwrap();
    pg.execute("SET search_path = one, public").unwrap();
    let mut call = pg.prepare("SELECT amount() + next_amount()").unwrap();
    assert_eq!(integers(&mut call), [3]);

    pg.execute("SET search_path = two, public").unwrap();
    assert_eq!(integers(&mut call), [4]);
}

#[test]
fn the_parameters_of_a_function_are_not_parameters_of_the_statement() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    pg.execute(
        "CREATE FUNCTION add(integer, integer) RETURNS integer LANGUAGE sql AS $$SELECT $1 + $2$$",
    )
    .unwrap();

    let mut without_parameters = pg.prepare("SELECT add(40, 2)").unwrap();
    assert_eq!(without_parameters.parameters_count(), 0);
    assert_eq!(integers(&mut without_parameters), [42]);

    // $1 of the statement is the second argument, which is $2 of the body.
    let mut with_parameter = pg.prepare("SELECT add(40, $1)").unwrap();
    assert_eq!(with_parameter.parameters_count(), 1);
    with_parameter
        .bind_at(std::num::NonZero::new(1).unwrap(), Value::from_i64(2))
        .unwrap();
    assert_eq!(integers(&mut with_parameter), [42]);
}

#[test]
fn a_parameter_of_the_statement_can_be_an_argument_that_the_body_does_not_use() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    pg.execute(
        "CREATE FUNCTION first_of(integer, integer) RETURNS integer LANGUAGE sql AS $$SELECT $1$$",
    )
    .unwrap();

    // The expression of the call has $2 only. $1 keeps its number.
    let mut call = pg.prepare("SELECT first_of($2, $1)").unwrap();
    call.bind_at(std::num::NonZero::new(1).unwrap(), Value::from_i64(5))
        .unwrap();
    call.bind_at(std::num::NonZero::new(2).unwrap(), Value::from_i64(6))
        .unwrap();
    assert_eq!(integers(&mut call), [6]);
}

#[test]
fn another_connection_calls_a_new_function() {
    let db = TempDatabase::builder().build();
    let writer = db.connect_postgres();
    let reader = db.connect_postgres();
    let error = reader.prepare("SELECT amount()").err().unwrap();
    assert_eq!(error.to_string(), "no such function: amount");

    writer
        .execute("CREATE FUNCTION amount() RETURNS integer LANGUAGE sql AS $$SELECT 1$$")
        .unwrap();
    assert_eq!(
        integers(&mut reader.prepare("SELECT amount()").unwrap()),
        [1]
    );
}

#[test]
fn explain_shows_the_expression_of_a_call() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    pg.execute("CREATE TABLE t (a integer)").unwrap();
    pg.execute(
        "CREATE FUNCTION add_one(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1 + 1$$",
    )
    .unwrap();
    let plan = pg
        .prepare("EXPLAIN SELECT add_one(a) FROM t")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    // The plan is the plan of `SELECT a + 1 FROM t`: it scans the table and
    // calls no function.
    assert!(!plan.is_empty());
    assert!(!format!("{plan:?}").contains("add_one"), "{plan:?}");
}

/// The expansion of a body in a body uses the stack, so tursopg limits how
/// many bodies a call can go through.
#[test]
fn a_call_through_too_many_function_bodies_fails() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    // CREATE FUNCTION expands the calls of the body when it checks the body.
    pg.execute("SET check_function_bodies = off").unwrap();
    pg.execute("CREATE FUNCTION link0() RETURNS integer LANGUAGE sql AS $$SELECT 1$$")
        .unwrap();
    for link in 1..=1000 {
        pg.execute(format!(
            "CREATE FUNCTION link{link}() RETURNS integer LANGUAGE sql AS $$SELECT link{}()$$",
            link - 1
        ))
        .unwrap();
    }

    let rows = pg
        .prepare("SELECT link10()")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows[0][0].as_int(), Some(1));
    let error = pg.execute("SELECT link1000()").unwrap_err();
    assert_eq!(error.sqlstate, "54001");
    assert_eq!(error.to_string(), "stack depth limit exceeded");
}

/// PostgreSQL compares a row on the left side of IN with each item of the
/// list on its own. With 9 such lists of 256 items, one in the other, it has
/// the parameter 256^9 times. That is more than a count can be.
#[test]
fn a_parameter_in_many_nested_in_lists_of_rows_is_used_more_than_once() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    pg.execute("SET check_function_bodies = off").unwrap();
    let items = vec!["(true, 1)"; 256].join(", ");
    let mut body = "$1".to_string();
    for _ in 0..9 {
        body = format!("(({body}, 1) IN ({items}))");
    }
    pg.execute(format!(
        "CREATE FUNCTION nested(boolean) RETURNS boolean LANGUAGE sql AS $$SELECT {body}$$"
    ))
    .unwrap();

    let error = pg.execute("SELECT nested(random() < 2)").unwrap_err();
    assert_eq!(error.sqlstate, "0A000");
    assert_eq!(
        error.to_string(),
        "a call of function \"nested\" with a volatile argument or a subquery that the body uses \
         more than once is not supported"
    );
}

/// PostgreSQL does not inline a function that would compute a volatile
/// argument more than once. tursopg says so also when the copies of the
/// argument would make the expression too large.
#[test]
fn a_large_volatile_argument_that_the_body_uses_many_times_is_not_supported() {
    let db = TempDatabase::builder().build();
    let pg = db.connect_postgres();
    pg.execute("CREATE SEQUENCE ids").unwrap();
    let body = vec!["$1"; 30].join(" + ");
    pg.execute(format!(
        "CREATE FUNCTION thirty_times(bigint) RETURNS bigint LANGUAGE sql AS $$SELECT {body}$$"
    ))
    .unwrap();
    let ones = vec!["1"; 400].join(", ");

    let error = pg
        .execute(format!(
            "SELECT thirty_times(nextval('ids') + greatest({ones}))"
        ))
        .unwrap_err();
    assert_eq!(error.sqlstate, "0A000");
    assert_eq!(
        error.to_string(),
        "a call of function \"thirty_times\" with a volatile argument or a subquery that the \
         body uses more than once is not supported"
    );
}

/// The walk of a body also uses the stack. With deep bodies, a call fails
/// before the stack of a server thread is full, which is 2 MiB.
#[test]
fn a_call_through_many_deep_function_bodies_fails_on_a_small_stack() {
    let on_a_server_stack = std::thread::Builder::new().stack_size(2 << 20);
    on_a_server_stack
        .spawn(|| {
            let db = TempDatabase::builder().build();
            let pg = db.connect_postgres();
            pg.execute("SET check_function_bodies = off").unwrap();
            pg.execute("CREATE FUNCTION deep0() RETURNS integer LANGUAGE sql AS $$SELECT 1$$")
                .unwrap();
            for function in 1..=85 {
                // The call of the function below is 30 levels down in the body.
                let body = format!(
                    "{}deep{}(){}",
                    "(1 + ".repeat(30),
                    function - 1,
                    ")".repeat(30)
                );
                pg.execute(format!(
                    "CREATE FUNCTION deep{function}() RETURNS integer LANGUAGE sql AS \
                     $$SELECT {body}$$"
                ))
                .unwrap();
            }

            let error = pg.execute("SELECT deep85()").unwrap_err();
            assert_eq!(error.sqlstate, "54001");
            assert_eq!(error.to_string(), "stack depth limit exceeded");
        })
        .unwrap()
        .join()
        .unwrap();
}
