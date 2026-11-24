//! The operators that PostgreSQL's jsonb has and the jsonb type of core does
//! not have.

use turso_core::schema::{OperatorArg, OperatorSignature};

/// FIXME(pg-catalog): This hand-codes the pg_operator entries `jsonb || jsonb` (function jsonb_concat), `jsonb - text`, `jsonb - integer` and `jsonb - text[]` (function jsonb_delete), `anynonarray || text` (function anytextcat) and `text || anynonarray` (function textanycat), and the resolution of an operand of unknown type: as jsonb for `||`, and by its value for `-`. A lookup in pg_operator by the types of the operands, in an expression tree that carries PostgreSQL types, replaces this.
/// The signatures of the operators of jsonb:
///
/// - `||` of two jsonb values runs jsonb_concat(). A string literal or a
///   parameter on one side is read as jsonb, as PostgreSQL reads an operand
///   of unknown type.
/// - `||` of a jsonb value and another value joins text, as SQLite's `||`
///   does.
/// - `jsonb - x` runs jsonb_delete(), which uses the kind of `x` to choose
///   what it deletes.
pub(super) fn operator_signatures() -> Vec<OperatorSignature> {
    use OperatorArg::{PlainValue, Untyped, Value};
    vec![
        jsonb_function("||", &[Value, Value], "jsonb_concat"),
        jsonb_function("||", &[Value, Untyped], "jsonb_concat"),
        jsonb_function("||", &[Untyped, Value], "jsonb_concat"),
        sqlite_operator("||", &[Value, PlainValue]),
        sqlite_operator("||", &[PlainValue, Value]),
        jsonb_function("-", &[Value, PlainValue], "jsonb_delete"),
    ]
}

/// The signature of an operator that runs the function `func`, which returns
/// a jsonb value.
fn jsonb_function(op: &'static str, args: &[OperatorArg], func: &str) -> OperatorSignature {
    OperatorSignature {
        op,
        args: args.to_vec(),
        func: Some(func.to_string()),
        result: Some("jsonb"),
    }
}

/// The signature of an operator that SQLite runs.
fn sqlite_operator(op: &'static str, args: &[OperatorArg]) -> OperatorSignature {
    OperatorSignature {
        op,
        args: args.to_vec(),
        func: None,
        result: None,
    }
}
