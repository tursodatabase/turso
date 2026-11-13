use rustc_hash::FxHashMap as HashMap;
use turso_core::schema::Schema;
use turso_core::sync::Arc;
use turso_core::translate::emitter::DoubleQuotedDml;
use turso_core::translate::emitter::Resolver;
use turso_core::translate::expr::expr_type::*;
use turso_core::{DatabaseCatalog, RwLock, SymbolTable};
use turso_parser::ast;
use turso_parser::ast::{Cmd, OneSelect, ResultColumn, Stmt};
use turso_sqlite::SqliteDialect;

#[test]
fn literals_and_casts() {
    check(&[
        ("1", builtin(BuiltinType::Integer)),
        ("1.5", builtin(BuiltinType::Decimal)),
        ("-1.5", builtin(BuiltinType::Decimal)),
        ("abs(-1.5)", builtin(BuiltinType::Decimal)),
        ("'a'", ExprType::Untyped),
        ("x'00'", builtin(BuiltinType::Blob)),
        ("NULL", ExprType::Null),
        ("CAST(1 AS TEXT)", builtin(BuiltinType::Text)),
        ("CAST(1 AS BLOB)", builtin(BuiltinType::Blob)),
        ("CAST(1 AS cents)", custom("cents", &[])),
        ("CAST(1 AS numeric(10, 2))", custom("numeric", &["10", "2"])),
        // numeric needs a precision and a scale, so without them this is
        // SQLite's NUMERIC.
        ("CAST(1 AS numeric)", builtin(BuiltinType::Numeric)),
        (
            "CAST(1 AS cents[])",
            ExprType::array(custom("cents", &[]), 1),
        ),
    ]);
}

#[test]
fn values_from_several_branches() {
    check(&[
        (
            "CASE WHEN 1 THEN 1 ELSE 2.5 END",
            builtin(BuiltinType::Decimal),
        ),
        (
            "CASE WHEN 1 THEN CAST(1 AS REAL) ELSE 2.5 END",
            builtin(BuiltinType::Real),
        ),
        ("CASE WHEN 1 THEN 'a' END", ExprType::Untyped),
        (
            "coalesce(NULL, 'a', CAST(1 AS TEXT))",
            builtin(BuiltinType::Text),
        ),
        ("coalesce(1, 'a')", builtin(BuiltinType::Any)),
        (
            "coalesce(CAST(1 AS cents), CAST(2 AS cents))",
            custom("cents", &[]),
        ),
        (
            "coalesce(CAST(1 AS numeric(10, 2)), CAST(2 AS numeric(10, 2)))",
            custom("numeric", &["10", "2"]),
        ),
        (
            "coalesce(CAST(1 AS numeric(10, 2)), CAST(2 AS numeric(5, 1)))",
            custom("numeric", &[]),
        ),
        // A custom value and a plain one have the custom type's stored type.
        (
            "coalesce(CAST(1 AS cents), 1)",
            builtin(BuiltinType::Integer),
        ),
        // Except that a number mixed with numeric is a numeric, so the
        // values compare in numeric order.
        (
            "coalesce(CAST(1 AS numeric(10, 2)), 0)",
            custom("numeric", &[]),
        ),
        (
            "coalesce(CAST(1 AS numeric(10, 2)), 0.5)",
            custom("numeric", &[]),
        ),
        (
            "coalesce(CAST(1 AS numeric(10, 2)), CAST(0.5 AS REAL))",
            custom("numeric", &[]),
        ),
        // Two different custom types have their common stored type.
        (
            "coalesce(CAST(1 AS cents), CAST(2 AS smallint))",
            builtin(BuiltinType::Integer),
        ),
        ("iif(1, NULL, 2)", builtin(BuiltinType::Integer)),
    ]);
}

#[test]
fn arithmetic() {
    check(&[
        ("1 + 2", builtin(BuiltinType::Integer)),
        ("1 + 2.5", builtin(BuiltinType::Real)),
        ("0.5 + 2.5", builtin(BuiltinType::Real)),
        ("1 + 'a'", builtin(BuiltinType::Numeric)),
        ("-CAST(1 AS REAL)", builtin(BuiltinType::Real)),
        // cents declares no `+`, so the statement fails with "operator does not exist".
        ("CAST(1 AS cents) + 1", ExprType::Unknown),
        // Nor any other operator.
        ("-CAST(1 AS cents)", ExprType::Unknown),
        ("~CAST(1 AS cents)", ExprType::Unknown),
        ("CAST(1 AS cents) & 1", ExprType::Unknown),
        ("-CAST(1 AS numeric(10, 2))", custom("numeric", &[])),
        ("-CAST(1 AS smallint)", builtin(BuiltinType::Integer)),
        ("CAST(1 AS smallint) << 1", builtin(BuiltinType::Integer)),
        ("abs(CAST(1 AS cents))", builtin(BuiltinType::Integer)),
        ("1 || 2", builtin(BuiltinType::Text)),
    ]);
}

#[test]
fn arrays_never_nest() {
    check(&[
        (
            "array_agg(CAST(1 AS INTEGER[]))",
            ExprType::array(builtin(BuiltinType::Integer), 2),
        ),
        (
            "array_element(CAST(1 AS INTEGER[][]), 1)",
            ExprType::array(builtin(BuiltinType::Integer), 1),
        ),
        (
            "array_element(CAST(1 AS INTEGER[]), 1)",
            builtin(BuiltinType::Integer),
        ),
        (
            "CAST(1 AS INTEGER[]) || 2",
            ExprType::array(builtin(BuiltinType::Integer), 1),
        ),
    ]);
}

fn check(cases: &[(&str, ExprType)]) {
    let schema = test_schema();
    let syms = SymbolTable::new();
    let database_schemas = RwLock::new(HashMap::default());
    let attached_databases = RwLock::new(DatabaseCatalog::new());
    let namespaces = turso_core::Qualifiers::default();
    let temp_database = RwLock::new(None);
    let resolver = Resolver::new(
        &schema,
        &database_schemas,
        &temp_database,
        &attached_databases,
        &namespaces,
        &syms,
        true,
        DoubleQuotedDml::Enabled,
        Arc::new(SqliteDialect),
        None,
        turso_core::security::roles::RoleId::SUPERUSER,
    );
    let env = TypeEnv::new(None, &resolver);
    let mismatches: Vec<String> = cases
        .iter()
        .filter_map(|(sql, expected)| {
            let actual = type_of(&parse_expr(sql), &env);
            (actual != *expected).then(|| format!("  {sql}: expected {expected:?}, got {actual:?}"))
        })
        .collect();
    assert!(
        mismatches.is_empty(),
        "wrong types:\n{}",
        mismatches.join("\n")
    );
}

/// CREATE TYPE accepts a DECODE only when this gives a kind.
#[test]
fn value_kind_of_every_type() {
    let schema = test_schema();
    let cases = [
        (builtin(BuiltinType::Integer), Some(BuiltinType::Integer)),
        (builtin(BuiltinType::Real), Some(BuiltinType::Real)),
        (builtin(BuiltinType::Decimal), Some(BuiltinType::Real)),
        (builtin(BuiltinType::Numeric), Some(BuiltinType::Numeric)),
        (builtin(BuiltinType::Text), Some(BuiltinType::Text)),
        (builtin(BuiltinType::Blob), Some(BuiltinType::Blob)),
        (builtin(BuiltinType::Any), None),
        (ExprType::Untyped, Some(BuiltinType::Text)),
        (ExprType::Null, None),
        (ExprType::Unknown, None),
        (
            ExprType::array(builtin(BuiltinType::Integer), 1),
            Some(BuiltinType::Blob),
        ),
        (custom("cents", &[]), Some(BuiltinType::Integer)),
        // numeric compares in its own order, which no affinity gives.
        (custom("numeric", &["10", "2"]), None),
    ];
    for (ty, expected) in cases {
        assert_eq!(value_kind(&ty, &schema), expected, "{ty:?}");
    }
}

fn parse_expr(sql: &str) -> ast::Expr {
    let select_sql = format!("SELECT {sql}");
    let Ok(Some(Cmd::Stmt(Stmt::Select(select)))) =
        turso_core::dialect::parse_statement(&SqliteDialect, &select_sql)
    else {
        panic!("not a SELECT: {sql}");
    };
    let OneSelect::Select { mut columns, .. } = select.body.select else {
        panic!("not a plain SELECT: {sql}");
    };
    let ResultColumn::Expr(expr, _) = columns.remove(0) else {
        panic!("not an expression: {sql}");
    };
    *expr
}

fn builtin(ty: BuiltinType) -> ExprType {
    ExprType::Builtin(ty)
}

/// A custom type from the test schema, with its parameters as written.
fn custom(name: &str, params: &[&str]) -> ExprType {
    let schema = test_schema();
    ExprType::Custom {
        type_def: Arc::clone(schema.get_type_def_unchecked(name).unwrap()),
        params: params.iter().map(|p| Box::new(parse_expr(p))).collect(),
    }
}

/// The built-in types plus `cents`.
fn test_schema() -> Schema {
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();
    schema
        .add_type_from_sql(
            &SqliteDialect,
            "CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100",
            &SymbolTable::new(),
        )
        .unwrap();
    schema
}
