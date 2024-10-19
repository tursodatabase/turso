use super::ast::Operator;
use super::tokenizer::{SqlTokenKind, SqlTokenStream};

pub(crate) fn peek_operator(input: &SqlTokenStream) -> Option<(Operator, u8)> {
    match input.peek_kind(0) {
        Some(SqlTokenKind::Eq) => Some((Operator::Eq, 2)),
        Some(SqlTokenKind::Neq) => Some((Operator::NotEq, 2)),
        Some(SqlTokenKind::Lt) => Some((Operator::Lt, 2)),
        Some(SqlTokenKind::Le) => Some((Operator::LtEq, 2)),
        Some(SqlTokenKind::Gt) => Some((Operator::Gt, 2)),
        Some(SqlTokenKind::Ge) => Some((Operator::GtEq, 2)),
        Some(SqlTokenKind::And) => Some((Operator::And, 1)),
        Some(SqlTokenKind::Or) => Some((Operator::Or, 0)),
        Some(SqlTokenKind::Like) => Some((Operator::Like, 0)),
        Some(SqlTokenKind::Plus) => Some((Operator::Plus, 3)),
        Some(SqlTokenKind::Minus) => Some((Operator::Minus, 3)),
        Some(SqlTokenKind::Asterisk) => Some((Operator::Multiply, 4)),
        Some(SqlTokenKind::Slash) => Some((Operator::Divide, 4)),
        _ => None,
    }
}
