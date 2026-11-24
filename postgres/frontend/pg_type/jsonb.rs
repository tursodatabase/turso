//! The operators that PostgreSQL's jsonb has and the jsonb type of core does
//! not have.

use turso_core::schema::{OperatorArg, OperatorSignature};

/// The signatures of the operators of jsonb. `jsonb - x` runs jsonb_delete(),
/// which uses the kind of the value of `x` to choose what it deletes.
pub(super) fn operator_signatures() -> Vec<OperatorSignature> {
    use OperatorArg::{PlainValue, Value};
    // FIXME(pg-catalog): this hand-codes the pg_operator entries
    // `jsonb - text`, `jsonb - integer` and `jsonb - text[]`, whose function
    // is jsonb_delete, and it resolves the type of the right operand by its
    // value. A lookup in pg_operator by the types of the operands, in an
    // expression tree that carries PostgreSQL types, replaces this.
    vec![jsonb_function("-", &[Value, PlainValue], "jsonb_delete")]
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
