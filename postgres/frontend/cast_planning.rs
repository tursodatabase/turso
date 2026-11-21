//! Applies the casts of casts.rs to the expressions of a statement.
//!
//! FIXME(pg-catalog): This file is temporary. An expression does not carry its type, so this file reads the type of a cast operand from the type core gives it, which for a type that is not registered is only a name (`cast_rule`, `named_cast_type`), and it has no way to convert a value where the cast is but to rewrite the expression into a call of the function `pg_cast` (`conversion_call`). Once float4, float8 and text are registered types and the expression tree is typed, the engine asks the target type for the cast with the source type in hand, calls `resolve` and `convert` of casts.rs itself, and this file goes away.

use crate::casts::{convert, resolve, CastType, CastType::*, Resolution};
use crate::error::sqlstate_error;
use turso_core::ast;
use turso_core::dialect::{CastContext, CastDecision, CastSource, TypeIdentity, TypeRef};
use turso_core::{LimboError, Register, Result, Value};
use turso_pg_parser::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED;

/// Decides a CAST of `operand` to `target` as PostgreSQL does when `CASTS`
/// covers both types, and fails when PostgreSQL has no such cast.
pub(crate) fn apply_cast(
    operand: &ast::Expr,
    source: &dyn Fn() -> CastSource,
    target: &TypeRef,
    context: CastContext,
) -> Result<CastDecision> {
    let Some(cast) = table_cast(source, target, context)? else {
        return Ok(CastDecision::EngineRules);
    };
    let operand = match resolve(cast.source, cast.target, cast.context)? {
        Resolution::Convert if needs_conversion(cast.source, cast.target) => {
            conversion_call(operand, cast.source, cast.target)
        }
        _ => operand.clone(),
    };
    Ok(CastDecision::Applied(operand))
}

/// A cast between two types of `CASTS`, in the context it is resolved in.
struct TableCast {
    source: CastType,
    target: CastType,
    context: CastContext,
}

/// The cast from the type `source` gives to `target`, written in `context`,
/// or None when the engine decides the cast of one of the two types.
fn table_cast(
    source: &dyn Fn() -> CastSource,
    target: &TypeRef,
    context: CastContext,
) -> Result<Option<TableCast>> {
    let target_place = match context {
        CastContext::Assignment => NamePlace::Column,
        _ => NamePlace::CastTarget,
    };
    let CastRule::Table(target) = cast_rule(target, target_place)? else {
        return Ok(None);
    };
    let (source, context) = match source() {
        // A string literal or a parameter takes the input of the target type.
        CastSource::Untyped => (Text, CastContext::Explicit),
        CastSource::Typed(source) => match cast_rule(&source, NamePlace::Value)? {
            CastRule::Table(source) => (source, context),
            CastRule::Engine => return Ok(None),
        },
        // FIXME(pg-catalog): Core cannot name the type of every expression, such as the result of most functions, so such a value is cast by the rules of the engine. `bool_cast` of pg_type/boolean.rs still decides its cast to boolean. A typed expression tree, where every expression has a registered type, removes this case.
        CastSource::Unnamed => return Ok(None),
    };
    Ok(Some(TableCast {
        source,
        target,
        context,
    }))
}

fn conversion_call(operand: &ast::Expr, source: CastType, target: CastType) -> ast::Expr {
    let name = |name: &str| ast::Expr::Literal(ast::Literal::String(format!("'{name}'")));
    let args = [operand.clone(), name(source.name()), name(target.name())]
        .into_iter()
        .map(Box::new)
        .collect();
    ast::Expr::FunctionCall {
        name: ast::Name::exact("pg_cast".to_string()),
        distinctness: None,
        args,
        order_by: vec![],
        within_group: vec![],
        filter_over: ast::FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    }
}

/// `pg_cast(value, source, target)`: the value converted from the type named
/// `source` to the type named `target`.
pub(crate) fn exec_pg_cast(args: &[Register]) -> Result<Value> {
    let value = args[0].get_value();
    let (Some(source), Some(target)) = (type_arg(&args[1]), type_arg(&args[2])) else {
        return Err(LimboError::InternalError(
            "pg_cast needs the names of two types".to_string(),
        ));
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    convert(value, source, target)
}

fn type_arg(arg: &Register) -> Option<CastType> {
    match arg.get_value() {
        Value::Text(name) => CastType::from_name(name.as_str()),
        _ => None,
    }
}

/// Whether the engine's own store of the value as the target type gives the
/// right value, which makes the conversion function unnecessary: the type
/// checks and encodes a value that is text.
fn needs_conversion(source: CastType, target: CastType) -> bool {
    !matches!(
        (source, target),
        (Text, Timestamp | TimestampTz) | (Timestamp | TimestampTz, Timestamp | TimestampTz)
    )
}

/// Who decides a cast from or to a type.
enum CastRule {
    /// `CASTS` decides, and a pair of types it does not list has no cast.
    Table(CastType),
    /// The rules of the engine decide, as before there was a cast table.
    Engine,
}

/// Who decides a cast from or to `ty`. Every type is in one of four groups:
/// a type of `CASTS`, a type of `ENGINE_RULE_TYPES`, a type that is not
/// built in, or a built-in type that tursopg cannot cast, which fails with
/// 0A000. `place` says where the name of a type that is not registered is
/// written.
fn cast_rule(ty: &TypeRef, place: NamePlace) -> Result<CastRule> {
    // FIXME(pg-catalog): pg_cast has rows for some arrays, but the engine casts every array by its own rules. Array types registered in pg_type, with typelem, let CASTS list them.
    if ty.array_dimensions > 0 {
        return Ok(CastRule::Engine);
    }
    let (cast_type, name) = match &ty.identity {
        // FIXME(pg-catalog): The engine casts a type made by CREATE TYPE or CREATE DOMAIN by its own rules, where PostgreSQL casts an enum only from and to text and a domain as its base type. pg_type rows for these types, with typtype and typbasetype, let this lookup decide them.
        TypeIdentity::Registered(type_def) if !type_def.is_builtin => return Ok(CastRule::Engine),
        TypeIdentity::Registered(type_def) => (
            registered_cast_type(&type_def.name),
            type_def.name.to_ascii_lowercase(),
        ),
        TypeIdentity::Name(name) => {
            let name = name.to_ascii_lowercase();
            if is_lowered_type_name(&name, place) {
                return Ok(CastRule::Engine);
            }
            (named_cast_type(&name, place), name)
        }
    };
    if let Some(cast_type) = cast_type {
        return Ok(CastRule::Table(cast_type));
    }
    if ENGINE_RULE_TYPES.contains(&name.as_str()) {
        return Ok(CastRule::Engine);
    }
    Err(sqlstate_error(
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        format!("casts of type {name} are not supported"),
        None,
    ))
}

/// The type of `CASTS` that is the built-in registered type `name`. A
/// registered type has one name here, whatever alias the statement wrote and
/// wherever it wrote it.
fn registered_cast_type(name: &str) -> Option<CastType> {
    Some(match name {
        "smallint" => Int2,
        "int4" => Int4,
        "bigint" => Int8,
        "boolean" => Bool,
        "timestamp" => Timestamp,
        "timestamptz" => TimestampTz,
        // FIXME(pg-catalog): numeric is registered only with a type modifier. Without one it is REAL, which `is_lowered_type_name` leaves to the engine. A numeric type that needs no modifier lets every numeric go through CASTS.
        "numeric" => Numeric,
        // FIXME(pg-catalog): varchar with a length is cast as text, and its length is checked when the value is stored. A pg_cast row for varchar, with the length coercion function, replaces this.
        "varchar" => Text,
        _ => return None,
    })
}

/// Where the name of a type that is not registered is written. The translator
/// and core lower several PostgreSQL types to one SQLite type name, and which
/// types a name stands for depends on the place.
#[derive(Clone, Copy, PartialEq, Eq)]
enum NamePlace {
    /// The type of a column, as the schema keeps it.
    Column,
    /// The type a CAST names.
    CastTarget,
    /// The type core gives an expression.
    Value,
}

/// FIXME(pg-catalog): This guesses a PostgreSQL type from the name of a type that is not registered. The translator lowers float4 and float8 to REAL and varchar without a length to TEXT, and core gives an expression the SQLite name of its type, so the same name is a different type in a different place. Once float4, float8 and text are registered types, as the integer types are, they are read by `registered_cast_type`, and this function and `NamePlace` go away.
/// The type of `CASTS` that the lowercase name `name` of a type that is not
/// registered stands for.
fn named_cast_type(name: &str, place: NamePlace) -> Option<CastType> {
    use NamePlace::{CastTarget, Column, Value};
    Some(match (name, place) {
        // FIXME(pg-catalog): An integer written in the statement and the result of a function have no registered type, and core types them INTEGER. They are read as int4, also when PostgreSQL types them bigint, so `count(*)::boolean` is accepted where PostgreSQL has no cast from bigint. The target type still checks its range. Result types from pg_proc, and a literal typed by its size, give the expression its real type.
        ("integer", Value) => Int4,
        ("float4", _) => Float4,
        ("float8", _) => Float8,
        // FIXME(pg-catalog): A float4 column is REAL in the schema, so it is only checked for the range of float8, and a float4 expression is typed float8. A registered float4 type fixes both.
        ("real", Column | Value) => Float8,
        // A decimal literal, which core types NUMERIC.
        ("numeric", Value) => Numeric,
        // FIXME(pg-catalog): The translator lowers a CAST to varchar, char, name, json, jsonb, inet, cidr, macaddr and xml to a CAST to TEXT, so all of them are cast as text here. Registered types that keep their name in the CAST replace this.
        ("text", _) => Text,
        (_, Column | CastTarget | Value) => return None,
    })
}

/// FIXME(pg-catalog): These built-in types have no rows in `CASTS`, so the engine decides their casts by `Dialect::text_input_types_refuse_other_kinds` and `check_cast_from_text_input_type` of core, and by the CAST function of the type. A type moves from this list to `CASTS` when its pg_cast rows and their conversions are written: date and time first, then json and jsonb, then inet, cidr and the macaddr types. bytea, oid and the reg types need a registered type first. The list is empty when every built-in type is in pg_type and pg_cast.
/// The built-in types whose casts the engine decides.
const ENGINE_RULE_TYPES: &[&str] = &[
    "date",
    "time",
    "json",
    "jsonb",
    "uuid",
    "bytea",
    "inet",
    "cidr",
    "macaddr",
    "macaddr8",
    "oid",
    "regclass",
    "regtype",
    "regproc",
    "regprocedure",
    "regoper",
    "regoperator",
    "regconfig",
    "regdictionary",
    "regnamespace",
    "regrole",
    "regcollation",
];

/// FIXME(pg-catalog): These are SQLite type names that the translator writes for PostgreSQL types `named_cast_type` cannot tell apart: an INTEGER column is an oid or a reg type, a CAST to INTEGER is a cast to one of those or to a serial type, a CAST to REAL is a cast to numeric, decimal or money, and BLOB is bytea. The engine casts them as it did before. Registered oid, numeric and bytea types, whose names reach the cast, replace this.
/// Whether the lowercase `name`, written at `place`, is a SQLite type name
/// that stands for a PostgreSQL type whose casts the engine decides.
fn is_lowered_type_name(name: &str, place: NamePlace) -> bool {
    match name {
        "integer" => place != NamePlace::Value,
        "real" => place == NamePlace::CastTarget,
        "blob" | "any" => true,
        _ => false,
    }
}
