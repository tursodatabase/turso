pub mod ast;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod token;

pub use parser::{MAX_COMPOUND_SELECT, MAX_EXPR_DEPTH, TOO_MANY_COMPOUND_TERMS_MESSAGE};

type Result<T, E = error::Error> = std::result::Result<T, E>;
