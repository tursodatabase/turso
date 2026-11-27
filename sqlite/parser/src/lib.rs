pub mod complete;
pub mod lexer;
pub mod parser;
pub mod token;

pub use parser::{MAX_COMPOUND_SELECT, MAX_EXPR_DEPTH, TOO_MANY_COMPOUND_TERMS_MESSAGE};

type Result<T, E = turso_ast::error::Error> = std::result::Result<T, E>;
