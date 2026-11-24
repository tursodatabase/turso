//! The operators that PostgreSQL's jsonb has and the jsonb type of core does
//! not have.

use turso_core::schema::{OperatorArg, OperatorSignature};

/// The signatures of the operators of jsonb:
///
/// - `||` of two jsonb values runs jsonb_concat(). A string literal or a
///   parameter on one side is read as jsonb, as PostgreSQL reads an operand
///   of unknown type.
/// - `||` of a jsonb value and another value joins text, as SQLite's `||`
///   does.
/// - `jsonb - x` runs jsonb_delete(), which uses the kind of the value of `x`
///   to choose what it deletes.
pub(super) fn operator_signatures() -> Vec<OperatorSignature> {
    use OperatorArg::{PlainValue, Untyped, Value};
    // FIXME(pg-catalog): this hand-codes the pg_operator entries
    // `jsonb || jsonb` (function jsonb_concat), `anynonarray || text`
    // (function anytextcat), `text || anynonarray` (function textanycat),
    // `jsonb - text`, `jsonb - integer` and `jsonb - text[]` (function
    // jsonb_delete). It reads a string literal or a parameter next to `||`
    // as jsonb, and it resolves the type of the right operand of `-` by its
    // value. A lookup in pg_operator by the types of the operands, in an
    // expression tree that carries PostgreSQL types, replaces this.
    vec![
        OperatorSignature::call("||", &[Value, Value], "jsonb_concat", "jsonb"),
        OperatorSignature::call("||", &[Value, Untyped], "jsonb_concat", "jsonb"),
        OperatorSignature::call("||", &[Untyped, Value], "jsonb_concat", "jsonb"),
        sqlite_operator("||", &[Value, PlainValue]),
        sqlite_operator("||", &[PlainValue, Value]),
        OperatorSignature::call("-", &[Value, PlainValue], "jsonb_delete", "jsonb"),
    ]
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
