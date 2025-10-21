use turso_core::Value;
use turso_parser::ast::{Expr, Literal};

use crate::runner::env::ShadowTablesMut;

pub mod assertion;
pub mod plan;
pub mod property;
pub mod query;

/// Shadow trait for types that can be "shadowed" in the simulator environment.
/// Shadowing is a process of applying a transformation to the simulator environment
/// that reflects the changes made by the query or operation represented by the type.
/// The result of the shadowing is typically a vector of rows, which can be used to
/// update the simulator environment or to verify the correctness of the operation.
/// The `Result` type is used to indicate the type of the result of the shadowing
/// operation, which can vary depending on the type of the operation being shadowed.
/// For example, a `Create` operation might return an empty vector, while an `Insert` operation
/// might return a vector of rows that were inserted into the table.
pub(crate) trait Shadow {
    type Result;
    fn shadow(&self, tables: &mut ShadowTablesMut<'_>) -> Self::Result;
}

pub(crate) fn value_to_literal(value: &Value) -> Literal {
    match value {
        Value::Null => Literal::Null,
        Value::Integer(i) => Literal::Numeric(i.to_string()),
        Value::Float(f) => Literal::Numeric(format!("{:.15}", f)),
        Value::Text(t) => Literal::String(t.to_string()),
        Value::Blob(b) => Literal::Blob(format!("X'{}'", hex::encode(b))),
    }
}
