pub mod ast;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod token;

pub use parser::{MAX_EXPR_DEPTH, MAX_EXPR_NESTING};

type Result<T, E = error::Error> = std::result::Result<T, E>;
