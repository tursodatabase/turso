//! Parser error types

use miette::Diagnostic;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Diagnostic)]
pub enum Error {
    #[error("Unexpected end of input")]
    #[diagnostic(code(parser::eof))]
    UnexpectedEof,

    #[error("Unexpected token: {token} at position {position}")]
    #[diagnostic(code(parser::unexpected_token))]
    UnexpectedToken { token: String, position: usize },

    #[error("Invalid number: {value}")]
    #[diagnostic(code(parser::invalid_number))]
    InvalidNumber { value: String },

    #[error("Unterminated string literal")]
    #[diagnostic(code(parser::unterminated_string))]
    UnterminatedString,

    #[error("Unterminated dollar quote: {tag}")]
    #[diagnostic(code(parser::unterminated_dollar_quote))]
    UnterminatedDollarQuote { tag: String },

    #[error("Invalid dollar parameter: ${0}")]
    #[diagnostic(code(parser::invalid_parameter))]
    InvalidParameter(String),

    #[error("Syntax error: {0}")]
    #[diagnostic(code(parser::syntax))]
    SyntaxError(String),

    #[error("Parser error: {0}")]
    #[diagnostic(code(parser::generic))]
    Generic(String),
}
